
TOP = $(shell pwd)/..

REQUIRES = configpp

include $(shell echo $${PREFIX-/usr})/share/smartmet/devel/makefile.inc

DEFINES = -DUNIX -D_REENTRANT

INCLUDES += -Iinclude

LIBS += $(PREFIX_LDFLAGS) \
	-lsmartmet-spine \
	-lsmartmet-macgyver \
	-lsmartmet-timeseries \
	-lpqxx \
	$(REQUIRED_LIBS) \
	-lbz2 -lz \
	-lpthread \
	-lm \
	-ldl

LIBWFS_INCLUDES := -I../avi
LIBWFS_LDFLAGS := ../avi.so

INCLUDES := $(LIBWFS_INCLUDES) $(INCLUDES)

TEST_PREPARE_TARGETS := cnf/valid.conf
TEST_FINISH_TARGETS := dummy
TEST_CLEAN_TARGETS := dummy
TEST_DB_DIR := $(shell pwd)/tmp-test-database
TEST_RUNNER :=

ifdef CI
GEONAMES_HOST_EDIT := sed -e 's|"@SMARTMET_TEST@"|"$(TEST_DB_DIR)"|g'
TEST_PREPARE_TARGETS += start-test-db
TEST_FINISH_TARGETS += stop-test-db
TEST_CLEAN_TARGETS := clean-test-db
else
GEONAMES_HOST_EDIT := sed -e 's|"@SMARTMET_TEST@"|"smartmet-test"|g'
endif

obj/%.o : %.cpp ; @echo Compiling $<
	@mkdir -p obj
	$(CXX) $(CFLAGS) $(INCLUDES) -c -MD -MF $(patsubst obj/%.o, obj/%.d.new, $@) -o $@ $<
	@sed -e "s|^$(notdir $@):|$@:|" $(patsubst obj/%.o, obj/%.d.new, $@) >$(patsubst obj/%.o, obj/%.d, $@)
	@rm -f $(patsubst obj/%.o, obj/%.d.new, $@)



TESTSUITE_SRCS = $(wildcard *.cpp)
TESTSUITE_OBJS = $(patsubst %.cpp,obj/%.o,$(TESTSUITE_SRCS))
TESTSUITE_TARGETS = $(patsubst %.cpp,%.test,$(TESTSUITE_SRCS))

all:

clean:
	rm -rf obj/*.o obj/*.d
	rm -rf $(TESTSUITE_TARGETS)
	-$(MAKE) $(TEST_CLEAN_TARGETS)

test:	$(TEST_PREPARE_TARGETS) $(TESTSUITE_TARGETS)
	@ok=true; \
	for test in $(TESTSUITE_TARGETS); do \
		if ! ./$$test --log_level=message ; then ok=false; fi; \
	done; \
	$(MAKE) $(TEST_FINISH_TARGETS)
	$$ok;

%.test : obj/%.o ; @echo "Building $@"
	$(CXX) -o $@ $(CFLAGS) $(INCLUDES) $< -Ltestsuite $(LIBWFS_LDFLAGS) $(LIBS)

CFLAGS += -I include -I $(TOP)/libwfs

test-database:
	@-$(MAKE) stop-test-db
	rm -rf tmp-test-database
	if ! /usr/share/smartmet/test/db/create-local-db.sh $(TEST_DB_DIR) >tmp-geonames-db.log 2>&1 ; then \
	    cat tmp-geonames-db.log; \
	    false; \
	fi

start-test-db: test-database
	/usr/share/smartmet/test/db/test-db-ctl.sh $(TEST_DB_DIR) start -w

stop-test-db:
	-/usr/share/smartmet/test/db/test-db-ctl.sh $(TEST_DB_DIR) stop

clean-test-db:
	-if test -d tmp-test-database; then $(MAKE) stop-test-db; fi;
	rm -rf tmp-test-database;
	rm -f tmp-geonames-db.log;


cnf/valid.conf:
	$(GEONAMES_HOST_EDIT) $@.in >$@

dummy:

ifneq ($(wildcard obj/*.d),)
-include $(wildcard obj/*.d)
endif

.PHONY: cnf/valid.conf
