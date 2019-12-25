%%%-------------------------------------------------------------------
%% @doc eclickhouse public API
%% @end
%%%-------------------------------------------------------------------

-module(eclickhouse).

-behaviour(gen_server).

-include_lib("kernel/include/logger.hrl").

-export([
  start_link/2,
  init/1,
  handle_call/3,
  handle_cast/2,
  handle_info/2,
  terminate/2
]).


-export([
  start_conn/2, 
  stop_conn/1, 
  list/0,
  get_state/1
]).

-export([
  insert/2, insert/3
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






start_link(Name, Conf) -> gen_server:start_link({local, Name}, ?MODULE, Conf, []).
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Gen Server api
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% infos
handle_info(timeout, S)                  -> timeout_(S);
handle_info(_Msg, S = #{u := U})         -> {noreply, S, ttl(U)}.
%% casts
handle_cast({insert_async, Maps}, S)     -> insert_async(S, Maps);
handle_cast(_Req, S = #{u := U})         -> {noreply, S, ttl(U)}.
%% calls
handle_call({insert_sync,  Maps}, _F, S) -> insert_sync(S, Maps);
handle_call(get_state, _From, S)         -> get_state_(S);
handle_call(_Req, _From, S = #{u := U})  -> {reply, ?e(unknown_msg), S, ttl(U)}.
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
init(Conf = #{write_period := WTime}) ->
  Now   = ?now,
  Until = Now + WTime,
  S = #{
    conf => Conf, 
    bulk => #{}, 
    bulk_status => ok,
    size => 0, 
    stat => #{}, 
    u => Until
  },
  {ok, S, ttl(Now, Until)}.

%
terminate(_Reason, _S) ->
  ok.


%
ttl(Until) ->
  ttl(?now, Until).
ttl(Now, Until) ->
  case Until - Now of
    Value when Value >= 0 -> 1000 * Value; %% To miliseconds
    _ -> 0
  end.


timeout_(S = #{size := 0, conf := #{write_period := WrPeriod}}) -> 
  Now  = ?now,
  NewU = Now + WrPeriod,
  {noreply, S#{u := NewU}, ttl(Now, NewU)};
timeout_(S = #{bulk := Bulk, 
               bulk_status := BulkStatus,
               conf := #{write_period := WrPeriod, db := Db}, 
               stat := Stat}) -> 
  Now  = ?now,
  NewU = Now + WrPeriod,
  NewS0 = S#{size := 0, bulk := #{}, bulk_status := ok, u := Now + WrPeriod},
  %% Time to log wrong bulk status
  NewStat0 = case BulkStatus of
    ok  -> Stat;
    {err, {BulkErrCode, _}} ->
      ?LOG_WARNING("bulk status ~p", [BulkStatus]),
      update_stat(Stat, BulkErrCode)
  end,
  NewS = case Bulk /= #{} of 
    true -> 
      case write_db(Db, Bulk) of
        {ok, _Result} -> 
            NewS0#{stat := update_stat(NewStat0, ok)};
        {err, {Code, _Reason}} -> 
            NewS0#{stat := update_stat(NewStat0, Code)}
      end;
    false ->
      NewS0#{stat := update_stat(Stat, bulk_size_mistake)}
  end,
  {noreply, NewS, ttl(Now, NewU)}.





%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% eclickhouse:start_conn(ech_test, #{enabled_fields => [id, name], bulk_size => 5, write_period => 2, db => #{url => "http://localhost:8123/?database=test_db}}).
start_conn(ConnName, Conf = #{enabled_fields := EnabledFields,
                              bulk_size      := BulkSize,
                              write_period   := WrPeriod}) 
  when is_atom(ConnName),
       is_list(EnabledFields), 
       is_integer(BulkSize),
       is_integer(WrPeriod) -> 

  Child = #{
      id        => ConnName,
      start     => {?MODULE, start_link, [ConnName, Conf]},
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



%
insert_async(S = #{conf := #{enabled_fields := EnabledFields, bulk_size := BSize, 
                             write_period := WrPeriod, db := Db}, 
                   stat := Stat, size := Size, 
                   bulk := Bulk, bulk_status := BulkStatus}, Maps) ->

  {NewBulkStatus0, NewBulk} = construct_bulk(EnabledFields, Bulk, Maps),
  NewBulkStatus = case BulkStatus == ok of true -> NewBulkStatus0; false -> BulkStatus end,
  NewSize = Size + length(Maps), 

  Now = ?now,
  NewS = #{u := NewU} = case NewSize >= BSize of
    true ->
      U = Now + WrPeriod,
      NewS0 = S#{size := 0, bulk := #{}, bulk_status := ok, u := U},
      %% Time to log wrong bulk status
      NewStat0 = case BulkStatus of
        ok -> Stat;
        {err, {BulkErrCode, _}} ->
          ?LOG_WARNING("bulk status ~p", [BulkStatus]),
          update_stat(Stat, BulkErrCode)
      end,
      case write_db(Db, NewBulk) of
        {ok, _Result}    -> NewS0#{stat := update_stat(NewStat0, ok)};
        {err, {Code,_R}} -> NewS0#{stat := update_stat(NewStat0, Code)}
      end;
    false ->
      S#{size := NewSize, bulk := NewBulk, bulk_status := NewBulkStatus}
  end,
 
  {noreply, NewS, ttl(Now, NewU)}.


%
insert_sync(S = #{conf := #{enabled_fields := EnabledFields, db := Db}, u := U}, Maps) ->
  Now = ?now,
  {BulkStatus, NewBulk} = construct_bulk(EnabledFields, _Bulk = #{}, Maps),
  Reply = write_db(Db, NewBulk), 
  {reply, #{bulk_status => BulkStatus, clickhouse_reply => Reply}, S, ttl(Now, U)}.


%
construct_bulk(all, Bulk, Maps) ->
  FilterFieldsFun = fun(K, V, {Fields, Values}) -> {[K|Fields], [V|Values]} end,

  BulkFun = fun(Map, AccBulk) ->
    case maps:fold(FilterFieldsFun, {[],[]}, Map) of
      {Fields, Values} when Fields /= [] ->
        case AccBulk of
          #{Fields := Data} -> AccBulk#{Fields := [Values|Data]};
          _ -> AccBulk#{Fields => [Values]}
        end;
      _ -> AccBulk
    end
  end,

  NewBulk = lists:foldl(BulkFun, Bulk, Maps),
  {ok, NewBulk};

 
construct_bulk(EnabledFields, Bulk, Maps) ->

  FilterFieldsFun =
    fun(K, V, Acc = {Fields, Values}) ->
      case lists:member(K, EnabledFields) of
        true  -> {[K|Fields], [V|Values]};
        false -> Acc
      end
    end,

  BulkFun = fun(Map, {Status, AccBulk}) ->
    {Fields, Values} =  maps:fold(FilterFieldsFun, {[],[]}, Map),
    NewStatus = 
      case maps:size(Map) == length(Fields) of 
        true  -> Status; 
        false -> ?e(some_fields_not_enabled) 
      end, 
    case Fields /= [] of
      true ->
        case AccBulk of
          #{Fields := Data} -> AccBulk#{Fields := [Values|Data]};
          _ -> {NewStatus, AccBulk#{Fields => [Values]}}
        end;
      false -> {NewStatus, AccBulk}
    end
  end,

  {BulkStatus, NewBulk} = lists:foldl(BulkFun, {ok, Bulk}, Maps),
  {BulkStatus, NewBulk}.


%
write_db(#{url := Url, table := Table}, Bulk) ->
  SqlBin = prepare_sql(Table, Bulk),
  case ibrowse:send_req(Url, [], post, SqlBin) of
    {ok, "200", _Headers, Body} -> {ok, Body};
    {ok, HttpCode, _Headers, Body} ->
      Error = #{http_code => HttpCode, clickhouse_reply => Body, bulk => Bulk},
      ?LOG_ERROR("sql error ~p", [Error]),
      ?e(sql_error, Error);
    HttpError -> 
      ?LOG_ERROR("http error ~p", [HttpError]),
      ?e(http_error, HttpError)
  end.



prepare_sql(Table, Bulk) ->
  SqlFun  = fun(K,V, Acc) -> [sqerl:sql({insert, Table, {K, V}}, true)|Acc] end,
  SqlList = maps:fold(SqlFun, [], Bulk),
  << <<SqlPart/binary, "\t">> || SqlPart <- SqlList >>.


update_stat(Stat, Key) ->
  maps:update_with(Key, fun(N) -> N + 1 end, 1, Stat).


%
get_state(ConnName) ->
  gen_server:call(ConnName, get_state).
get_state_(S = #{u := U, stat := Stat, size := Size, conf := Conf}) ->
  {reply, #{until_write_time => U - ?now, current_bulk_size => Size, conf => Conf, stat => Stat}, S, ttl(U)}.
