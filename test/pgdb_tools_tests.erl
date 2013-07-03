-module(pgdb_tools_tests).
-include_lib("eunit/include/eunit.hrl").
-include_lib("epgsql/include/pgsql.hrl").
-define(POOLSIZE, 20).


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
    ?assertMatch(ok, wait_for_pool(?POOLSIZE)).

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



           




