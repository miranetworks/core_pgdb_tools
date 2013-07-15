-module(pgdb_tools).
-export([
    transduce/2,

    equery/2, equery/3,
    squery/1, squery/2,

    with_transaction/1, with_transaction/2,
    transaction_equery/3,

    get_connection/0, get_connection/1, get_connection/3,

    return_connection/1, return_connection/2,

    date_to_string/1,
    timestamp_to_string/1,

    proplist_to_hstore/1,
    hstore_matrix_to_proplist/1
]).
-ifdef(TEST).
-export([handle_error/2]).
-endif.

-include_lib("epgsql/include/pgsql.hrl").

-define(POOL, default).

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
%% @doc Wrapper for pgsql:equery/3 using the default pool
%%
%% It also catches exotic errors and try to handle it in a sane way.
%%
-spec equery(Sql::iolist()|binary(), Params::[any()]) ->  
    {ok, Columns::[any()], Rows::[any()]} |
    {ok, Count::non_neg_integer()} |
    {ok, Count::non_neg_integer(), Columns::[any()], Rows::[any()]} |
    {error, Error::any()}.

equery(Sql, Params) ->
    equery(Sql, Params, ?POOL).

%%
%% @doc Wrapper for pgsql:equery/3 using a specified pool 
%%
-spec equery(Sql::iolist()|binary(), Params::[any()], Pool::atom()) ->
    {ok, Columns::[any()], Rows::[any()]} |
    {ok, Count::non_neg_integer()} |
    {ok, Count::non_neg_integer(), Columns::[any()], Rows::[any()]} |
    {error, Error::any()}.

equery(Sql, Params, Pool) ->
    case get_connection(Pool) of
        {ok, Con} ->
            Result = pgsql:equery(Con, Sql, Params),
            ok = handle_error(Con, Result),
            ok = return_connection(Con, Pool),
            Result;
        Other -> 
            Other
    end.

%%
%% @doc Wrapper for pgsql:squery/2 using the default pool
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
    squery(Sql, ?POOL).


%%
%% @doc Wrapper for pgsql:squery/2 using a specified pool
%%
%% It also catches exotic errors and try to handle it in a sane way.
%% Important: squery does not map results to Erlang types, as equery does.
%%
-spec squery(Sql::iolist()|binary(), Pool::atom()) ->
    {ok, Columns::[any()], Rows::[any()]} |
    {ok, Count::non_neg_integer()} |
    {ok, Count::non_neg_integer(), Columns::[any()], Rows::[any()]} |
    {error, Error::any()}.

squery(Sql, Pool) ->
    case get_connection(Pool) of
        {ok, Con} -> 
            Result = pgsql:squery(Con, Sql),
            ok = handle_error(Con, Result),
            ok = return_connection(Con, Pool),
            Result;
        Other -> 
            Other
    end.


%%
%% @doc Wrapper for pgsql:with_transaction/2 using the default pool.
%%
-spec with_transaction(Fun::fun((Con::pid()) -> Result::any())) ->  
    Result::any() |
    {rollback, Error::any()} |
    {error, Error::any()}.

with_transaction(Fun) when is_function(Fun) ->
    with_transaction(Fun, ?POOL).

%%
%% @doc Wrapper for pgsql:with_transaction/2 using a specified pool.
%%
%% It is important to only use transaction_equery/3 within the fun for database interaction.
%% Use the connection argument passed to the fun as the 3rd argument to transaction_equery/3.
%%
-spec with_transaction(Fun::fun((Con::pid()) -> Result::any()), Pool::atom()) ->  
    Result::any() |
    {rollback, Error::any()} |
    {error, Error::any()}.

with_transaction(Fun, Pool) when is_function(Fun) ->
    case get_connection(Pool) of
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
%% @doc Get a database connection from the default pool, using the default retry and timeout.
%%
-spec get_connection() ->
    {ok, Pid::pid()} |
    {error, any()}.

get_connection() ->
    get_connection(?POOL).

%%
%% @doc Get a database connection from the speified pool, using the default retry and timeout. 
%%
-spec get_connection(Pool::atom()) ->
    {ok, Pid::pid()} |
    {error, any()}.

get_connection(Pool) ->
    DefaultRetryCount = 3,
    % DefaultTimeout specified in POOL configuration.
    DefaultTimeout = 1000,
    get_connection(Pool, DefaultRetryCount, DefaultTimeout).

%%
%% @doc Get a database connection from the specified pool, trying RetryCount times with Timeout timeouts
%%
-spec get_connection(Pool::atom(), RetryCount::non_neg_integer(), Timeout::non_neg_integer()) ->
    {ok, Pid::pid()} | 
    {error, any()}.

get_connection(Pool, 0, Timeout) ->
    pgsql_pool:get_connection(Pool, Timeout);
get_connection(Pool, RetryCount, Timeout)
when is_integer(RetryCount), RetryCount > 0,
     is_integer(Timeout), Timeout >= 0 ->

    case pgsql_pool:get_connection(Pool, Timeout) of
        {error, timeout} -> get_connection(Pool, RetryCount-1, Timeout);
        Other -> Other
    end.

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
%% @doc Convert a proplist to its HSTORE representation
%%
-spec proplist_to_hstore(PropList::list({Key::binary()|string(), Value::binary()|string()})) -> iolist().

proplist_to_hstore(PropList) ->
    Map = fun({K, V}) ->
        io_lib:format("['~s','~s']", [K, V])
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

