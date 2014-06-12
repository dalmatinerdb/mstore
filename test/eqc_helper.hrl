-ifdef(EQC_CI).
-define(OUT(P),  on_output(fun(S,F) -> io:fwrite(user, S, F) end, P)).
-else.
-define(OUT(P),
        on_output(fun
                      (".", []) ->
                          io:fwrite(user, <<"\e[0;32m*\e[0m">>, []);
                     ("x", []) ->
                          io:format(user, <<"\e[0;33mx\e[0m">>, []);
                     ("Failed! ", []) ->
                          io:format(user, <<"\e[0;31mFailed! \e[0m">>, []);
                     (S, F) ->
                          io:format(user, S, F)
                  end, P)).
-endif.

run_test_() ->
    [{exports, E} | _] = module_info(),
    E1 = [{atom_to_list(N), N} || {N, 0} <- E],
    E2 = [{N, A} || {"prop_" ++ N, A} <- E1],
    [{"Running " ++ N ++ " propperty test",
      ?_assert(quickcheck(numtests(500,  ?OUT(?MODULE:A()))))}
     || {N, A} <- E2].
