-module(pgdb_tools).
-export([
    transduce/2,

    equery/2, equery/3,
    squery/1, squery/2,
    
    get_connection/0, get_connection/1, get_connection/3,

    return_connection/1, return_connection/2
]).
-include_lib("epgsql/include/pgsql.hrl").

-define(POOL, default).

%% 
%% @doc Return a list of proplists representing the Columns and Rows supplied
%%
-spec transduce(Colums::[#column{}], Rows::[tuple()]) -> [list({binary(), any()})].

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
-spec equery(Sql::iolist()|binary(), Params::[any()]) ->  {ok, Columns::[any()], Rows::[any()]} |
                                                 {ok, Count::non_neg_integer()} |
                                                 {ok, Count::non_neg_integer(), Columns::[any()], Rows::[any()]} |
                                                 {error, Error::any()}.

equery(Sql, Params) ->
    equery(Sql, Params, ?POOL).

%%
%% @doc Wrapper for pgsql:equery/3 using a specified pool 
%%
-spec equery(Sql::iolist()|binary(), Params::[any()], Pool::atom()) ->  {ok, Columns::[any()], Rows::[any()]} |
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
%% @doc Wrapper for pgsql:squery/2
%%
%% It also catches exotic errors and try to handle it in a sane way.
%% Important: squery does not map results to Erlang types, as equery does.
%%
-spec squery(Sql::iolist()|binary()) ->  {ok, Columns::[any()], Rows::[any()]} |
                                {ok, Count::non_neg_integer()} |
                                {ok, Count::non_neg_integer(), Columns::[any()], Rows::[any()]} |
                                {error, Error::any()}.

squery(Sql) ->
    squery(Sql, ?POOL).

-spec squery(Sql::iolist()|binary(), Pool::atom()) ->  {ok, Columns::[any()], Rows::[any()]} |
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
-spec get_connection() -> {ok, Pid::pid()} | {error, any()}.

get_connection() ->
    get_connection(?POOL).

%%
%% @doc Get a database connection from the speified pool, using the default retry and timeout. 
%%
-spec get_connection(Pool::atom()) -> {ok, Pid::pid()} | {error, any()}.

get_connection(Pool) ->
    DefaultRetryCount = 3,
    % DefaultTimeout specified in POOL configuration.
    DefaultTimeout = 1000,
    get_connection(Pool, DefaultRetryCount, DefaultTimeout).

%%
%% @doc Get a database connection from the specified pool, trying RetryCount times with Timeout timeouts
%%
-spec get_connection(Pool::atom(), RetryCount::non_neg_integer(), Timeout::non_neg_integer()) -> {ok, Pid::pid()} | {error, any()}.

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


