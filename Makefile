# Handle git submodules
GIT:=$(shell git -C "$(CURDIR)" rev-parse --git-dir 1>/dev/null 2>&1 \
        && command -v git)
ifneq ($(GIT),)
freshsubs:=$(shell git submodule update --init $(quiet_errors))
endif

all: run

run:
	@echo "Run with:"
	@echo ' $ python3 -m amifuse.fuse_fs \\'
	@echo "           --driver pfs3aio \\"
	@echo "           --image 'pfs.hdf' \\"
	@echo "           --mountpoint ./mnt &"
