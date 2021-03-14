%%%-------------------------------------------------------------------
%% @doc eclickhouse public API
%% @end
%%%-------------------------------------------------------------------

-module(eclickhouse).

-include_lib("kernel/include/logger.hrl").

-export([
  start_conn/2, 
  stop_conn/1, 
  list/0,
  get_state/1
]).

-export([
  insert/2, insert/3,
  req/2
]).



%% Point
-define(p,         list_to_binary(io_lib:format("Mod:~w line:~w",       [?MODULE, ?LINE]))).
-define(p(Reason), list_to_binary(io_lib:format("Mod:~w line:~w ~100P", [?MODULE, ?LINE, Reason, 300]))).
-define(e(ErrCode), {err, {ErrCode, ?p}}).
-define(e(ErrCode, Reason), {err, {ErrCode, ?p(Reason)}}).
-define(f, list_to_binary(atom_to_list(?FUNCTION_NAME))).
% Now
-define(now,   erlang:system_time(seconds)).
-define(mnow,  erlang:system_time(millisecond)).
-define(stime, bdtmp_misc:datetime_to_list()).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% eclickhouse:start_conn(ech_test, #{enabled_fields => [id, name], bulk_size => 5, write_period => 2, db => #{url => "http://localhost:8123/?database=test_db}}).
%% enabled_fields => [id, name]|all|{plain, [id, name]}
start_conn(ConnName, Conf = #{enabled_fields := EnabledFields,
                              bulk_size      := BulkSize,
                              write_period   := WrPeriod}) 
  when is_atom(ConnName),
       is_integer(BulkSize),
       is_integer(WrPeriod) -> 

  Child = #{
      id        => ConnName,
      start     => {eclickhouse_conn, start_link, [ConnName, Conf]},
      restart   => permanent,
      shutdown  => 5000,
      type      => worker},
  case supervisor:start_child(eclickhouse_sup, Child) of
    {ok, _} -> ok;
    Else -> Else
  end;

start_conn(_ConnName, _Conf) -> 
  ?e(wrong_conf).

%
stop_conn(ConnName) ->
  case supervisor:terminate_child(eclickhouse_sup, ConnName) of
    ok -> supervisor:delete_child(eclickhouse_sup, ConnName);
    El -> El
  end.

%
list() ->
  [Name || {Name,_,_,_} <- supervisor:which_children(eclickhouse_sup)].



%%
%% insert(test_conn, #{name => <<"aa">>, metric_a => 1111}).
%%
insert(Conn, Map) ->
  Opts = #{mode => async},
  insert(Conn, Map, Opts).


%% async
insert(Conn, Map, #{mode := async}) when is_map(Map) ->
  gen_server:cast(Conn, {insert_async, [Map]});
insert(Conn, Maps, #{mode := async}) when is_list(Maps) ->
  gen_server:cast(Conn, {insert_async, Maps});

%% sync (For testing only)
insert(Conn, Map, #{mode := sync}) when is_map(Map) ->
  gen_server:call(Conn, {insert_sync, [Map]});
insert(Conn, Maps, #{mode := sync}) when is_list(Maps) ->
  gen_server:call(Conn, {insert_sync, Maps}).


%%
req(Conn, Sql) ->
   gen_server:call(Conn, {req, Sql}).


%
get_state(ConnName) ->
  gen_server:call(ConnName, get_state).

