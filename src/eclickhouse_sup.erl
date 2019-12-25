%%%-------------------------------------------------------------------
%% @doc eclickhouse top level supervisor.
%% @end
%%%-------------------------------------------------------------------

-module(eclickhouse_sup).

-behaviour(supervisor).

%% API
-export([start_link/0]).

%% Supervisor callbacks
-export([init/1]).

-define(SERVER, ?MODULE).

%%====================================================================
%% API functions
%%====================================================================

start_link() ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, []).

%%====================================================================
%% Supervisor callbacks
%%====================================================================

init([]) ->
    SupFlags = #{
      strategy  => one_for_one,
      intensity => 1000, %% Never
      period    => 1},
    Children = [],
    {ok, {SupFlags, Children}}.

