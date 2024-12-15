#define _GNU_SOURCE 1
#include "asyncio.h"
#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#define log(fmt, ...) \
	fprintf(stderr, "%s:%d " fmt "\n", __FILE__, __LINE__, ##__VA_ARGS__)

#define die(cond, ...)                                          \
	do {                                                    \
		if (cond) {                                     \
			log("ERROR: `" #cond "` " __VA_ARGS__); \
			abort();                                \
		}                                               \
	} while (0)

static long assign_iocb(struct aio_ctx *aio)
{
	size_t i = 0;
	for (; i < aio->max_req && !TAILQ_EMPTY(&aio->iocbq); ++i) {
		struct iocb_ctx *ctx = TAILQ_FIRST(&aio->iocbq);
		aio->iocb_ptr[i] = &ctx->iocb;
		TAILQ_REMOVE(&aio->iocbq, ctx, link);
		TAILQ_INSERT_TAIL(&aio->waitq, ctx, link);
	}
	return i;
}

static struct iocb_ctx *get_iocb(struct aio_ctx *aio)
{
	struct iocb_ctx *ctx = TAILQ_FIRST(&aio->pool);
	TAILQ_REMOVE(&aio->pool, ctx, link);
	TAILQ_INSERT_TAIL(&aio->iocbq, ctx, link);
	aio->queued += 1;
	return ctx;
}

size_t page_size(void)
{
	return sysconf(_SC_PAGESIZE);
}

struct aio_ctx *aio_init(size_t max_req)
{
	struct aio_ctx *aio = calloc(1, sizeof(struct aio_ctx));
	die(!aio);

	aio->max_req = max_req;
	TAILQ_INIT(&aio->iocbq);
	TAILQ_INIT(&aio->waitq);
	TAILQ_INIT(&aio->pool);

	for (size_t i = 0; i < max_req; ++i) {
		struct iocb_ctx *ctx = calloc(1, sizeof(struct iocb_ctx));
		die(!ctx);
		TAILQ_INSERT_TAIL(&aio->pool, ctx, link);
	}

	aio->iocb_ptr = calloc(max_req, sizeof(struct iocb *));
	die(!aio->iocb_ptr);

	aio->event = calloc(max_req, sizeof(struct io_event));
	die(!aio->event);
	int rc = io_setup(max_req, &aio->ctx);
	die(rc != 0, "io_setup rc %d errno %d", rc, errno);

	return aio;
}

void aio_destroy(struct aio_ctx *aio)
{
	int rc = io_destroy(aio->ctx);
	if (rc != 0)
		log("io_destroy rc: %d errno: %d", rc, errno);
	TAILQ_CONCAT(&aio->pool, &aio->iocbq, link);
	TAILQ_CONCAT(&aio->pool, &aio->waitq, link);
	while (!TAILQ_EMPTY(&aio->pool)) {
		struct iocb_ctx *ctx = TAILQ_FIRST(&aio->pool);
		TAILQ_REMOVE(&aio->pool, ctx, link);
		free(ctx);
	}
	free(aio->event);
	free(aio->iocb_ptr);
	free(aio);
}

bool aio_full(struct aio_ctx *aio)
{
	return TAILQ_EMPTY(&aio->pool);
}

bool aio_empty(struct aio_ctx *aio)
{
	return TAILQ_EMPTY(&aio->iocbq);
}

size_t aio_pending(struct aio_ctx *aio)
{
	return aio->queued;
}

void aio_prepare_read(struct aio_ctx *aio, int fd, void *buf, size_t count,
		      uint64_t off)
{
	struct iocb_ctx *ctx = get_iocb(aio);
	io_prep_pread(&ctx->iocb, fd, buf, count, off);
	ctx->iocb.data = ctx;
}

void aio_prepare_write(struct aio_ctx *aio, int fd, void *buf, size_t count,
		       uint64_t off)
{
	struct iocb_ctx *ctx = get_iocb(aio);
	io_prep_pwrite(&ctx->iocb, fd, buf, count, off);
	ctx->iocb.data = ctx;
}

void aio_fsync(struct aio_ctx *aio, int fd)
{
	struct iocb_ctx *ctx = get_iocb(aio);
	io_prep_fsync(&ctx->iocb, fd);
	ctx->iocb.data = ctx;
}

int aio_submit(struct aio_ctx *aio)
{
	long n = assign_iocb(aio);
	if (n == 0)
		return 0;
	return io_submit(aio->ctx, n, aio->iocb_ptr);
}

int aio_wait(struct aio_ctx *aio, uint64_t timeout_ms)
{
	struct timespec ts = { .tv_sec = timeout_ms / 1000,
			       .tv_nsec = 1000000 * (timeout_ms % 1000) };
	struct timespec *pts = &ts;
	if (timeout_ms == UINT64_MAX)
		pts = NULL;
	int rc = io_getevents(aio->ctx, aio->queued, aio->queued, aio->event,
			      pts);
	if (rc < 0)
		log("io_getevents rc %d errno %d", rc, errno);

	for (int i = 0; i < rc; ++i) {
		aio->queued -= 1;
		struct io_event *e = &aio->event[i];
		struct iocb_ctx *ctx = e->data;
		// we assume it will never fail
		if (e->res2 != 0 || e->res != e->obj->u.c.nbytes) {
			log("aio fail, offset %lld expect bytes %lu read bytes %lu",
			    e->obj->u.c.offset, e->obj->u.c.nbytes, e->res);
			abort();
		}
		TAILQ_REMOVE(&aio->waitq, ctx, link);
		TAILQ_INSERT_TAIL(&aio->pool, ctx, link);
	}

	return rc;
}

int file_open(const char *path, bool direct, bool trunc)
{
	int flag = O_CREAT | O_RDWR;
	if (direct)
		flag |= O_DIRECT;
	if (trunc)
		flag |= O_TRUNC;
	return open(path, flag, 0644);
}

int file_read(int fd, void *buf, size_t n, uint64_t off)
{
	return pread(fd, buf, n, off);
}

int file_fdsync(int fd)
{
	return fdatasync(fd);
}

int file_close(int fd)
{
	return close(fd);
}

uint64_t file_size(int fd)
{
	struct stat st;
	int rc = fstat(fd, &st);
	die(rc != 0, "cant' stat fd %d, rc %d errno: %d", fd, rc, errno);
	return st.st_size;
}

int last_error(void)
{
	return errno;
}