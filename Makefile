BIN_DIR=_output/cmd/bin

all-local: build
clean:
	-rm -f ${BIN_DIR}/gotgt
install-exec-local:
	$(INSTALL_PROGRAM) gotgt $(bindir)

build:
	go build -o ${BIN_DIR}/gotgt
