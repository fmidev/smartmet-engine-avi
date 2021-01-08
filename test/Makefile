
TOP = $(shell pwd)/..

REQUIRES = configpp

include $(shell echo $${PREFIX-/usr})/share/smartmet/devel/makefile.inc

DEFINES = -DUNIX -D_REENTRANT

INCLUDES += -Iinclude

LIBS += -L$(libdir) \
	-lsmartmet-spine \
	-lsmartmet-macgyver \
	-lpqxx \
	$(REQUIRED_LIBS) \
	-lbz2 -lz \
	-lpthread \
	-lm \
	-ldl

LIBWFS_INCLUDES := -I../avi
LIBWFS_LDFLAGS := ../avi.so

INCLUDES := $(LIBWFS_INCLUDES) $(INCLUDES)

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

test:	$(TESTSUITE_TARGETS)
	@ok=true; \
	for test in $(TESTSUITE_TARGETS); do \
		if ! ./$$test --log_level=message ; then ok=false; fi; \
	done; \
	$$ok;

%.test : obj/%.o ; @echo "Building $@"
	$(CXX) -o $@ $(TESTSUITE_CFLAGS) $(INCLUDES) $< -Ltestsuite $(LIBWFS_LDFLAGS) $(LIBS)

CFLAGS += -I include -I $(TOP)/libwfs

ifneq ($(wildcard obj/*.d),)
-include $(wildcard obj/*.d)
endif
