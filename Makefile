REBAR = $(shell pwd)/rebar3

.PHONY: rel package

all: compile

publish:
	$(REBAR) as pkg upgrade
	$(REBAR) as pkg hex publish
	$(REBAR) upgrade

compile:
	$(REBAR) compile

clean:
	-rm -r .eunit
	$(REBAR) clean

distclean: clean
	$(REBAR) delete-deps

qc: clean all
	$(REBAR) as eqc qc

###
### Docs
###
docs:
	$(REBAR) edoc

##
## Developer targets
##

xref:
	$(REBAR) xref 

console: all
	erl -pa ebin deps/*/ebin -s libsniffle


##
## Dialyzer
##
APPS = kernel stdlib sasl erts ssl tools os_mon runtime_tools crypto inets \
	xmerl webtool snmp public_key mnesia eunit syntax_tools compiler
COMBO_PLT = $(HOME)/.mstore_combo_dialyzer_plt

check_plt: deps compile
	dialyzer --check_plt --plt $(COMBO_PLT) --apps $(APPS) \
		deps/*/ebin ebin

build_plt: deps compile
	dialyzer --build_plt --output_plt $(COMBO_PLT) --apps $(APPS) \
		deps/*/ebin ebin

dialyzer: deps compile
	@echo
	@echo Use "'make check_plt'" to check PLT prior to using this target.
	@echo Use "'make build_plt'" to build PLT prior to using this target.
	@echo
	@sleep 1
	dialyzer -Wno_return --plt $(COMBO_PLT) deps/*/ebin ebin | grep -v -f dialyzer.mittigate


cleanplt:
	@echo
	@echo "Are you sure?  It takes about 1/2 hour to re-build."
	@echo Deleting $(COMBO_PLT) in 5 seconds.
	@echo
	sleep 5
	rm $(COMBO_PLT)
