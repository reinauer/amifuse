# Handle git submodules
GIT:=$(shell git -C "$(CURDIR)" rev-parse --git-dir 1>/dev/null 2>&1 \
        && command -v git)
ifneq ($(GIT),)
freshsubs:=$(shell git submodule update --init $(quiet_errors))
endif

all: run

# Downloads directory and files
DOWNLOAD_DIR = Downloads
PFS_7Z = $(DOWNLOAD_DIR)/pfs.7z
PFS3AIO = $(DOWNLOAD_DIR)/pfs3aio
PFS_BENCH_BASELINE ?= ../amifuse-0.2
PFS_BENCH_IMAGE ?= pfs.hdf
PFS_BENCH_DRIVER ?= pfs3aio
PFS_BENCH_REPEAT ?= 3

# MD5 checksums for verification
PFS_7Z_MD5 = 305e6a720b88e03655cd9ff56d7950bc
PFS3AIO_MD5 = 2912d39e917903ca5d3cc9e9bdb6fb33

# Google Drive file IDs
PFS_7Z_FILEID = 1ORYiD1095LrJ7AdqFKzHwa3jSzkPDYtN
PFS3AIO_FILEID = 1mOUK3uVugWDxBzIat5YaDlGJpd5GtY6z

# Portable MD5 verification (works on Linux and macOS)
# Usage: $(call md5_cmd,file)
define md5_cmd
md5sum "$(1)" 2>/dev/null | cut -d' ' -f1 || md5 -q "$(1)" 2>/dev/null
endef
define verify_md5_cmd
actual=$$($(call md5_cmd,$(1))); \
[ "$$actual" = "$(2)" ]
endef
define md5_fail_msg
actual=$$($(call md5_cmd,$(1))); \
echo "$(1): FAILED (MD5 mismatch)"; \
echo "Expected MD5: $(2)"; \
echo "Got MD5: $$actual"
endef

# Create downloads directory
$(DOWNLOAD_DIR):
	mkdir -p $(DOWNLOAD_DIR)

# Download and verify pfs.7z
$(PFS_7Z): | $(DOWNLOAD_DIR)
	@if [ -f "$@" ] && $(call verify_md5_cmd,$@,$(PFS_7Z_MD5)); then \
		echo "$@ already downloaded and verified"; \
	else \
		echo "Downloading pfs.7z..."; \
		curl -sL -o $@ "https://drive.google.com/uc?export=download&id=$(PFS_7Z_FILEID)"; \
		if $(call verify_md5_cmd,$@,$(PFS_7Z_MD5)); then \
			echo "$@: OK"; \
		else \
			$(call md5_fail_msg,$@,$(PFS_7Z_MD5)); rm -f $@; exit 1; \
		fi \
	fi

# Download and verify pfs3aio
$(PFS3AIO): | $(DOWNLOAD_DIR)
	@if [ -f "$@" ] && $(call verify_md5_cmd,$@,$(PFS3AIO_MD5)); then \
		echo "$@ already downloaded and verified"; \
	else \
		echo "Downloading pfs3aio..."; \
		curl -sL -o $@ "https://drive.google.com/uc?export=download&id=$(PFS3AIO_FILEID)"; \
		if $(call verify_md5_cmd,$@,$(PFS3AIO_MD5)); then \
			echo "$@: OK"; \
		else \
			$(call md5_fail_msg,$@,$(PFS3AIO_MD5)); rm -f $@; exit 1; \
		fi \
	fi

download: $(PFS_7Z) $(PFS3AIO)

unpack: download
	@7z x $(PFS_7Z)
	@cp $(PFS3AIO) .

bench-pfs:
	@python3 tools/pfs_benchmark.py \
		--baseline "$(PFS_BENCH_BASELINE)" \
		--candidate . \
		--image "$(PFS_BENCH_IMAGE)" \
		--driver "$(PFS_BENCH_DRIVER)" \
		--repeat "$(PFS_BENCH_REPEAT)"

run:
	@echo "Run with:"
	@echo " $$ python3 -m amifuse mount pfs.hdf \\"
	@echo "           --driver pfs3aio \\"
	@echo "           --mountpoint ./mnt"
