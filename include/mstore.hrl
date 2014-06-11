%% Those two must match!
-define(BITS, 64).
-define(DATA_SIZE, (1+8)).

-define(NONE, 0).
-define(INT, 1).
-define(FLOAT,2).

-define(B2L(B), mstore_bin:to_list(B)).
-define(L2B(L), mstore_bin:from_list(L)).
