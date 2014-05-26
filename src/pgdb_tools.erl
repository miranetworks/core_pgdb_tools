-module(pgdb_tools).
-export([
    transduce/2,

    equery/2, equery/3, equery/4,
    squery/1, squery/2, squery/3,

    with_transaction/1, with_transaction/2, with_transaction/3,
    transaction_equery/3,

    get_connection/0, get_connection/2,

    return_connection/1, return_connection/2,

    date_to_string/1,
    timestamp_to_string/1,
    timestamp_to_now/1,

    proplist_to_hstore/1,
    hstore_matrix_to_proplist/1
]).
-ifdef(TEST).
-export([handle_error/2]).
-endif.

-include_lib("epgsql/include/pgsql.hrl").

-define(POOL, default).
-define(TIMEOUT, infinity).  % The maximum time (in ms) to wait for a connection from the pool.
-define(QT, "SaFe"). % The PostgreSQL Quote Tag to use with unsafe strings

%% 
%% @doc Return a list of proplists representing the Columns and Rows supplied
%%
-spec transduce(Colums::[#column{}], Rows::[tuple()]) ->
    [list({binary(), any()})].

transduce(_Colums, []) ->
    [];
transduce(Columns, Rows) ->
    ColumnNames = [ C#column.name || C <- Columns ],
    [ lists:zip(ColumnNames, erlang:tuple_to_list(Row)) || Row <- Rows].

%%
%% @doc Wrapper for pgsql:equery/3 using the default pool and timeout
%%
%% It also catches exotic errors and try to handle it in a sane way.
%%
-spec equery(Sql::iolist()|binary(), Params::[any()]) ->  
    {ok, Columns::[any()], Rows::[any()]} |
    {ok, Count::non_neg_integer()} |
    {ok, Count::non_neg_integer(), Columns::[any()], Rows::[any()]} |
    {error, Error::any()}.

equery(Sql, Params) ->
    equery(Sql, Params, ?POOL, ?TIMEOUT).

%%
%% @doc Wrapper for pgsql:equery/3 using the default pool and a specified timeout
%%
%% It also catches exotic errors and try to handle it in a sane way.
%%
-spec equery(Sql::iolist()|binary(), Params::[any()], Timeout::pos_integer()) ->  
    {ok, Columns::[any()], Rows::[any()]} |
    {ok, Count::non_neg_integer()} |
    {ok, Count::non_neg_integer(), Columns::[any()], Rows::[any()]} |
    {error, Error::any()}.

equery(Sql, Params, Timeout) ->
    equery(Sql, Params, ?POOL, Timeout).

%%
%% @doc Wrapper for pgsql:equery/3 using a specified pool and timeout
%%
-spec equery(Sql::iolist()|binary(), Params::[any()], Pool::atom(), Timeout::pos_integer()) ->
    {ok, Columns::[any()], Rows::[any()]} |
    {ok, Count::non_neg_integer()} |
    {ok, Count::non_neg_integer(), Columns::[any()], Rows::[any()]} |
    {error, Error::any()}.

equery(Sql, Params, Pool, Timeout) ->
    case get_connection(Pool, Timeout) of
        {ok, Con} ->
            Result = pgsql:equery(Con, Sql, Params),
            ok = handle_error(Con, Result),
            ok = return_connection(Con, Pool),
            Result;
        Other -> 
            Other
    end.

%%
%% @doc Wrapper for pgsql:squery/2 using the default pool and timeout
%%
%% It also catches exotic errors and try to handle it in a sane way.
%% Important: squery does not map results to Erlang types, as equery does.
%%
-spec squery(Sql::iolist()|binary()) ->
    {ok, Columns::[any()], Rows::[any()]} |
    {ok, Count::non_neg_integer()} |
    {ok, Count::non_neg_integer(), Columns::[any()], Rows::[any()]} |
    {error, Error::any()}.

squery(Sql) ->
    squery(Sql, ?POOL, ?TIMEOUT).


%%
%% @doc Wrapper for pgsql:squery/2 using the default pool and a specified timeout
%%
%% It also catches exotic errors and try to handle it in a sane way.
%% Important: squery does not map results to Erlang types, as equery does.
%%
-spec squery(Sql::iolist()|binary(), Timeout::pos_integer()) ->
    {ok, Columns::[any()], Rows::[any()]} |
    {ok, Count::non_neg_integer()} |
    {ok, Count::non_neg_integer(), Columns::[any()], Rows::[any()]} |
    {error, Error::any()}.

squery(Sql, Timeout) ->
    squery(Sql, ?POOL, Timeout).


%%
%% @doc Wrapper for pgsql:squery/2 using a specified pool and timeout
%%
%% It also catches exotic errors and try to handle it in a sane way.
%% Important: squery does not map results to Erlang types, as equery does.
%%
-spec squery(Sql::iolist()|binary(), Pool::atom(), Timeout::pos_integer()) ->
    {ok, Columns::[any()], Rows::[any()]} |
    {ok, Count::non_neg_integer()} |
    {ok, Count::non_neg_integer(), Columns::[any()], Rows::[any()]} |
    {error, Error::any()}.

squery(Sql, Pool, Timeout) ->
    case get_connection(Pool, Timeout) of
        {ok, Con} -> 
            Result = pgsql:squery(Con, Sql),
            ok = handle_error(Con, Result),
            ok = return_connection(Con, Pool),
            Result;
        Other -> 
            Other
    end.


%%
%% @doc Wrapper for pgsql:with_transaction/2 using the default pool and timeout.
%%
-spec with_transaction(Fun::fun((Con::pid()) -> Result::any())) ->  
    Result::any() |
    {rollback, Error::any()} |
    {error, Error::any()}.

with_transaction(Fun) when is_function(Fun) ->
    with_transaction(Fun, ?POOL, ?TIMEOUT).

%%
%% @doc Wrapper for pgsql:with_transaction/2 using the default pool and a specified timeout.
%%
-spec with_transaction(Fun::fun((Con::pid()) -> Result::any()), Timeout::pos_integer()) ->  
    Result::any() |
    {rollback, Error::any()} |
    {error, Error::any()}.

with_transaction(Fun, Timeout) when is_function(Fun) ->
    with_transaction(Fun, ?POOL, Timeout).

%%
%% @doc Wrapper for pgsql:with_transaction/2 using a specified pool and timeout
%%
%% It is important to only use transaction_equery/3 within the fun for database interaction.
%% Use the connection argument passed to the fun as the 3rd argument to transaction_equery/3.
%%
-spec with_transaction(Fun::fun((Con::pid()) -> Result::any()), Pool::atom(), Timeout::pos_integer()) ->  
    Result::any() |
    {rollback, Error::any()} |
    {error, Error::any()}.

with_transaction(Fun, Pool, Timeout) when is_function(Fun) ->
    case get_connection(Pool, Timeout) of
        {ok, Con} -> 
            Result = pgsql:with_transaction(Con, Fun),
            ok = return_connection(Con, Pool),
            Result;
        Other -> 
            Other
    end.

%%
%% @doc Wrapper for pgsql:equery/3 for use with with_transaction/2,3.
%%
-spec transaction_equery(Sql::iolist()|binary(), Params::[any()], Con::pid()) ->
    {ok, Columns::[any()], Rows::[any()]} |
    {ok, Count::non_neg_integer()} |
    {ok, Count::non_neg_integer(), Columns::[any()], Rows::[any()]} |
    {error, Error::any()}.

transaction_equery(Sql, Params, Con) ->
    Result = pgsql:equery(Con, Sql, Params),
    ok = handle_error(Con, Result),
    Result.


-spec handle_error(Con::pid(), Response::any()) -> ok.

handle_error(Con, Response) ->
    case Response of
        {error, #error{message=M}} -> % Standard error. Log and pass on.
            error_logger:error_msg("PSQL ERROR: ~s", [M]);
        {error, closed} -> % Log and fail.
            error_logger:error_msg("PSQL ERROR: closed", []);
        {error, sync_required} -> % According to docs "Error occured and pgsql:sync must be called"
            ok = error_logger:error_msg("PSQL ERROR: sync_required", []),
            pgsql:sync(Con); % Hopefully this fixes things
        _ -> % We do not log timeouts or successful results
            ok 
    end.


%%
%% @doc Get a database connection from the default pool, using the default timeout.
%%
-spec get_connection() ->
    {ok, Pid::pid()} |
    {error, any()}.

get_connection() ->
    get_connection(?POOL, ?TIMEOUT).

%%
%% @doc Get a database connection from the specified pool, using a specified timeout
%%
-spec get_connection(Pool::atom(), Timeout::pos_integer()) ->
    {ok, Pid::pid()} |
    {error, any()}.

get_connection(Pool, Timeout) ->
    pgsql_pool:get_connection(Pool, Timeout).

%%
%% @doc Return a connection to the default pool.
%%
-spec return_connection(Con::pid()) -> ok.

return_connection(Con) ->
    return_connection(Con, ?POOL).

%%
%% @doc Return a connection to the specified pool.
%%
-spec return_connection(Con::pid(), Pool::atom()) -> ok.

return_connection(Con, Pool) ->
    pgsql_pool:return_connection(Pool, Con).

%%
%% @doc Convert a Postgres date to its string representation.
%%
%% {Y, M, D} => "YYYY-MM-DD"
%%
-spec date_to_string(Date::calendar:date()) -> string().

date_to_string({Y,M,D})
when is_integer(Y), Y>0,
     is_integer(M), 12>=M, M>=1,
     is_integer(D), 31>=D, D>=1 ->
    lists:flatten(io_lib:format("~4.10.0B-~2.10.0B-~2.10.0B", [Y, M, D])).

%%
%% @doc Convert a Postgres timestamp to its string representation.
%%
%% {{Y, Mo, D}, {H, Mi, S}} => "YYYY-MO-DDTHH:MI:SS.mmm"
%%
-spec timestamp_to_string({Date::calendar:date(), {H::non_neg_integer(), M::non_neg_integer(), FloatSecs::float()}}) -> string().

timestamp_to_string({{Y,Mo,D}, {H, Mi, S}})
when is_integer(Y), is_integer(Mo), is_integer(D),
     is_integer(H), is_integer(Mi), is_float(S) ->
    lists:flatten(io_lib:format("~4.10.0B-~2.10.0B-~2.10.0BT~2.10.0B:~2.10.0B:~6.3.0f", [Y, Mo, D, H, Mi, S])).


%%
%% @doc Converts a Postgres database timestamp to a representation as returned by the erlang:now/0 function.
%%
%% {{Y, Mo, D}, {H, Mi, S}} => {{MegaSecs, Secs, MicroSecs}
%%       
%% @see erlang:now/0
%%
-spec timestamp_to_now({Date::calendar:date(), {H::non_neg_integer(), M::non_neg_integer(), FloatSecs::float()}}) -> 
    {MegaSecs::non_neg_integer(),
     Secs::non_neg_integer(),
     MicroSecs::non_neg_integer()}.

timestamp_to_now({{Y, Mo, D}, {H, Mi, FloatSecs}})
when is_integer(Y), is_integer(Mo), is_integer(D),
     is_integer(H), is_integer(Mi), is_float(FloatSecs) ->
    IntSecs = trunc(FloatSecs),
    TsInSeconds = calendar:datetime_to_gregorian_seconds({{Y, Mo, D}, {H, Mi, IntSecs}}) -
                  62167219200, % calendar:datetime_to_gregorian_seconds({{1970, 1, 1},{0,0,0}})

    MegaSecs = TsInSeconds div 1000000,
    Secs = TsInSeconds - MegaSecs * 1000000,
    MicroSecs = trunc(FloatSecs * 1000000 - IntSecs * 1000000), % Needs to be done this way to prevent precision loss.
    {MegaSecs, Secs, MicroSecs}.


%%
%% @doc Convert a proplist to its HSTORE representation
%%
-spec proplist_to_hstore(PropList::list({Key::binary()|string(), Value::binary()|string()})) -> iolist().

proplist_to_hstore(PropList) ->
    Map = fun({K, V}) ->
        io_lib:format("[$"?QT"$~s$"?QT"$,$"?QT"$~s$"?QT"$]", [K, V])
    end,

    ["hstore(ARRAY[", string:join(lists:map(Map, PropList), ", "), "]::text[])"].

%%
%% @doc Convert an HSTORE matrix representation to a proplist
%%
%% The hstore matrix representation is the result of the hstore_to_matrix() SQL function.
%%
-spec hstore_matrix_to_proplist(HstoreMatrix::list(list(binary()))) -> list({binary(), binary()}).

hstore_matrix_to_proplist(HstoreMatrix) ->
    lists:map(fun erlang:list_to_tuple/1, HstoreMatrix).

