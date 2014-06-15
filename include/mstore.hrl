%% Those two must match!
-define(BITS, 64).
-define(DATA_SIZE, (1+8)).

-define(NONE, 0).
-define(INT, 1).
-define(FLOAT,2).

-define(B2L(B), mmath_bin:to_list(B)).
-define(L2B(L), mmath_bin:from_list(L)).
