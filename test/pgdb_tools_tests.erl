-module(pgdb_tools_tests).
-include_lib("eunit/include/eunit.hrl").
-include_lib("epgsql/include/pgsql.hrl").
-define(POOLSIZE, 1).


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

wait_for_pool(0) ->
        ok;
wait_for_pool(PoolSize) ->
        case pgdb_tools:get_connection() of
                {ok, Con} ->
                        ?assertEqual(ok, wait_for_pool(PoolSize-1)),
                        ?assertEqual(ok, pgdb_tools:return_connection(Con)),
                        ok;
                {error, timeout} ->
                        ?assertEqual(ok, timer:sleep(1000)),
                        ?assertEqual(ok, wait_for_pool(PoolSize)),
                        ok

                end.


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

equery_same_results_test() ->
    Sql = "SELECT day, created_at, message
             FROM pgdb_tools_test
            WHERE int = $1
            ORDER BY day",

    {ok, Cols1, Rows1} = pgdb_tools:equery(Sql, [1]),
    {ok, Cols2, Rows2} = pgdb_tools:equery(Sql, [1], special),

    ?assertEqual(Cols1, Cols2),
    ?assertEqual(Rows1, Rows2).

delete_old_entries_test() ->
    Sql = "DELETE FROM pgdb_tools_test
            WHERE int > 3",
    ?assertMatch({ok,_}, pgdb_tools:squery(Sql)).

%sync_error_test() ->
%    {ok, C} = pgdb_tools:get_connection(),
%    {ok, S} = pgsql:parse(C, "INSERT INTO pgdb_tools_test (int, message) VALUES ($1, $2)"),
%    ?assertEqual(ok, pgsql:bind(C, S, [1, <<"foo">>])),
%    ?assertMatch({error, #error{code = <<"23505">>}}, pgsql:execute(C, S, 0)),
%    ?assertEqual({error, sync_required}, pgsql:bind(C, S, [4, <<"quux">>])),
%    ?assertEqual(ok, pgsql:sync(C)),
%    ?assertEqual(ok, pgsql:bind(C, S, [4, <<"quux">>])),
%    ?assertMatch({ok, _}, pgsql:execute(C, S, 0)),
%    ?assertEqual(ok, pgdb_tools:return_connection(C)).

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


