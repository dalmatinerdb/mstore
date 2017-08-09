rm -r libs
mkdir libs
cd libs
curl -Ol http://www.quviq.com/wp-content/uploads/2015/09/eqcmini-2.01.0.zip
unzip eqcmini-2.01.0.zip
cd ..
export ERL_LIBS=./libs 
./rebar3 dialyzer
./rebar3 eunit
./rebar3 as eqc eqc
