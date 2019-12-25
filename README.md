eclickhouse
=====

Erlang clickhouse async bulk inserter

Usage
-----
Rebar3:
  Add eclickhouse to deps in your rebar.config file.
  Add eclickhouse to applications in your .app.src file.

Start conn example:
```erlang
    start_clickhouse() ->
      Name = click, %% Conn name unique atom()
      Conf = #{
        enabled_fields => [ts, id, value, a], %% (drop field not in enabled_fields) of all
        bulk_size      => 1000, %% size bulk inserts to save
        write_period   => 3, % Sec max time to save small bulk
        db             => #{url => "http://localhost:8123/?database=test_db", 
                            table => test_table}
      },
      ok = eclickhouse:start_conn(Name, Conf).
```

Insert example:
```erlang
    eclickhouse:insert(click, #{id => 2, a => 1}).
```

Sync insert (for debug and testing)
```erlang
    eclickhouse:insert(click, #{id => 2, a => 1}, #{mode => sync}).
```


Get conn info and statistics example:
```erlang
    eclickhouse:get_state(click).
```

Stop connection
```erlang
    eclickhouse:stop_conn(click)
```
  
