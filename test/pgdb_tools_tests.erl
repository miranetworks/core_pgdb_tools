-module(pgdb_tools_tests).
-include_lib("eunit/include/eunit.hrl").
-include_lib("epgsql/include/pgsql.hrl").
-define(POOLSIZE, 1).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Helper functions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

ensure_started(App) ->
    case application:start(App) of
        ok ->
            ok;
        {error, {already_started, App}} ->
            ok
    end.

ensure_loaded(App) ->
    case application:load(App) of
        ok ->
            ok;
        {error, {already_loaded, App}} ->
            ok
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Tests
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

setup_test() ->
    error_logger:tty(false),

    ?assertEqual(ok, ensure_loaded(sasl)),
    ?assertEqual(ok, application:set_env(sasl, errlog_type, error)),
    ?assertEqual(ok, ensure_started(sasl)),

    ?assertEqual(ok, ensure_started(public_key)),
    ?assertEqual(ok, ensure_started(ssl)),
    ?assertEqual(ok, ensure_started(inets)),

    ?assertEqual(ok, ensure_started(epgsql)),
    ?assertEqual(ok, ensure_started(epgsql_pool)),


    ?assertMatch({ok, _}, epgsql_pool:start_pool(default, ?POOLSIZE,
                                                 [{database, "pgdb_tools"},
                                                  {username, "pgdb_tools"},
                                                  {port,5432},
                                                  {password, "password"},
                                                  {timeout, infinity}
                                                 ])),
    ?assertMatch({ok, _}, epgsql_pool:start_pool(special, ?POOLSIZE,
                                                 [{database, "pgdb_tools"},
                                                  {username, "pgdb_tools"},
                                                  {port,5432},
                                                  {password, "password"},
                                                  {timeout, infinity}
                                                 ])). 


transduce_test() ->
    Cols = [
        #column{name= <<"testcol1">>},
        #column{name= <<"testcol2">>},
        #column{name= <<"testcol3">>}
    ],
    Rows = [
        {<<"string1">>, 1, {2013, 1, 1}},
        {<<"string2">>, 2, {2013, 2, 2}}
    ],
    ?assertEqual([[{<<"testcol1">>, <<"string1">>}, {<<"testcol2">>, 1}, {<<"testcol3">>, {2013, 1, 1}}],
                  [{<<"testcol1">>, <<"string2">>}, {<<"testcol2">>, 2}, {<<"testcol3">>, {2013, 2, 2}}]
                 ],
                 pgdb_tools:transduce(Cols, Rows)).


transduce_empty_test() ->
    Cols = [
        #column{name= <<"testcol1">>},
        #column{name= <<"testcol2">>},
        #column{name= <<"testcol3">>}
    ],
    Rows = [],
    ?assertEqual([], pgdb_tools:transduce(Cols, Rows)).


connection_default_test() ->
    {ok, Con} = pgdb_tools:get_connection(),
    ?assertEqual(ok, pgdb_tools:return_connection(Con)).


connection_special_test() ->
    {ok, Con} = pgdb_tools:get_connection(special),
    ?assertEqual(ok, pgdb_tools:return_connection(Con, special)).


connection_starvation_test_() ->
    {timeout, 10, [fun () ->

     ?assertEqual(1, ?POOLSIZE),
     {ok, Con} = pgdb_tools:get_connection(),
     ?assertEqual({error, timeout}, pgdb_tools:get_connection()),
     ?assertEqual(ok, pgdb_tools:return_connection(Con))

     end]}.


squery_same_results_test() ->
    Sql = "SELECT day, created_at, message 
             FROM pgdb_tools_test
            ORDER BY day",
    {ok, Cols1, Rows1} = pgdb_tools:squery(Sql),
    {ok, Cols2, Rows2} = pgdb_tools:squery(Sql, special),

    ?assertEqual(Cols1, Cols2),
    ?assertEqual(Rows1, Rows2).


squery_starvation_test_() ->
    {timeout, 10, [fun () ->

     ?assertEqual(1, ?POOLSIZE),
     {ok, Con} = pgdb_tools:get_connection(),
     ?assertEqual({error, timeout}, pgdb_tools:squery("SELECT * FROM pgdb_tools_test")),
     ?assertEqual(ok, pgdb_tools:return_connection(Con))

     end]}.


equery_same_results_test() ->
    Sql = "SELECT day, created_at, message
             FROM pgdb_tools_test
            WHERE int = $1
            ORDER BY day",

    {ok, Cols1, Rows1} = pgdb_tools:equery(Sql, [1]),
    {ok, Cols2, Rows2} = pgdb_tools:equery(Sql, [1], special),

    ?assertEqual(Cols1, Cols2),
    ?assertEqual(Rows1, Rows2).


equery_starvation_test_() ->
    {timeout, 10, [fun () ->

     ?assertEqual(1, ?POOLSIZE),
     {ok, Con} = pgdb_tools:get_connection(),
     ?assertEqual({error, timeout}, pgdb_tools:equery("SELECT * FROM pgdb_tools_test WHERE $1 = $1", ["foo"])),
     ?assertEqual(ok, pgdb_tools:return_connection(Con))

     end]}.


delete_old_entries_test() ->
    Sql = "DELETE FROM pgdb_tools_test
            WHERE int > 3",
    ?assertMatch({ok,_}, pgdb_tools:squery(Sql)).


sync_error_handled_test() ->
    {ok, C} = pgdb_tools:get_connection(),
    
    % Break the connection
    {ok, S} = pgsql:parse(C, "INSERT INTO pgdb_tools_test (int, message) VALUES ($1, $2)"),
    ?assertEqual(ok, pgsql:bind(C, S, [1, <<"foo">>])),
    ?assertMatch({error, #error{code = <<"23505">>}}, pgsql:execute(C, S, 0)),

    % Hand broken connection back to pool
    ?assertEqual(ok, pgdb_tools:return_connection(C)),

    % Use the same connection in the next request. Must perform an equery, since squeries
    % will generate a badmatch in pgsql:squery/1 when {error, sync_required} is returned.
    % The first time the connection is used, it should return {error, sync_required}.
    ?assertEqual({error, sync_required}, pgdb_tools:equery("SELECT COUNT(*) FROM pgdb_tools_test", [])),
    % The next time it is used, it should work (i.e. the connection was synced.
    ?assertMatch({ok, _, [{3}]}, pgdb_tools:equery("SELECT COUNT(*) FROM pgdb_tools_test WHERE $1 = $1", ["foo"])).


handle_error_test() ->
    Error = #error{message="Test error"},
    % The {error, sync_required} case is tested by the sync_error_handled_test.
    ?assertEqual(ok, pgdb_tools:handle_error(unused, {error, Error})),
    ?assertEqual(ok, pgdb_tools:handle_error(unused, {error, closed})),
    ?assertEqual(ok, pgdb_tools:handle_error(unused, {ok, something})).


with_transaction_rollback_test() ->
    Sql = "SELECT int, message 
           FROM pgdb_tools_test
           ORDER BY int",
    ?assertMatch({ok, _, [{1,<<"One">>}, {2,<<"Two">>}, {3,<<"Three">>}]}, pgdb_tools:equery(Sql, [])),

    Fun = fun(Con) ->
            [{ok, 1} = pgdb_tools:transaction_equery("UPDATE pgdb_tools_test SET message=upper(message) WHERE int=$1", [X], Con) || X <- lists:seq(1,3)],
            {ok, _, [{1,<<"ONE">>}, {2,<<"TWO">>}, {3,<<"THREE">>}]} = pgdb_tools:transaction_equery(Sql, [], Con),
            throw(crap)
    end,
    ?assertEqual({rollback, crap}, pgdb_tools:with_transaction(Fun)),
    ?assertMatch({ok, _, [{1,<<"One">>}, {2,<<"Two">>}, {3,<<"Three">>}]}, pgdb_tools:equery(Sql, [])),
    ok.


with_transaction_commit_test() ->
    Sql = "SELECT int, message 
           FROM pgdb_tools_test
           ORDER BY int",
    ?assertMatch({ok, _, [{1,<<"One">>}, {2,<<"Two">>}, {3,<<"Three">>}]}, pgdb_tools:equery(Sql, [])),

    Fun = fun(Con) ->
            Updates = [{ok, 1} = pgdb_tools:transaction_equery("UPDATE pgdb_tools_test SET message=reverse(message) WHERE int=$1", [X], Con) || X <- lists:seq(1,3)],
            {ok, length(Updates)}
    end,
    ?assertEqual({ok, 3}, pgdb_tools:with_transaction(Fun)),
    ?assertMatch({ok, _, [{1,<<"enO">>}, {2,<<"owT">>}, {3,<<"eerhT">>}]}, pgdb_tools:equery(Sql, [])),

    ?assertEqual({ok, 3}, pgdb_tools:with_transaction(Fun)),
    ?assertMatch({ok, _, [{1,<<"One">>}, {2,<<"Two">>}, {3,<<"Three">>}]}, pgdb_tools:equery(Sql, [])),
    ok.


with_transaction_starvation_test_() ->
    {timeout, 10, [fun () ->

     ?assertEqual(1, ?POOLSIZE),
     {ok, Con} = pgdb_tools:get_connection(),
     ?assertEqual({error, timeout}, pgdb_tools:with_transaction(fun(Con) -> ok end)),
     ?assertEqual(ok, pgdb_tools:return_connection(Con))

     end]}.
