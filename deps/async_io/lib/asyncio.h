// SPDX-License-Identifier: MIT
/*
 * Author: Abby Cin
 * Mail: abbytsing@gmail.com
 * Create Time: 2024-11-25 16:39:43
 */

#ifndef AIO_1732523983_H_
#define AIO_1732523983_H_

#include <libaio.h>
#include <stdbool.h>
#include <stdint.h>
#include <sys/queue.h>

struct iocb_ctx {
	struct iocb iocb;
	TAILQ_ENTRY(iocb_ctx) link;
};

TAILQ_HEAD(iocbq, iocb_ctx);

struct aio_ctx {
	size_t max_req;
	size_t queued;
	io_context_t ctx;
	struct iocbq iocbq;
	struct iocbq waitq;
	struct iocbq pool;
	struct iocb **iocb_ptr;
	struct io_event *event;
};

size_t page_size(void);

struct aio_ctx *aio_init(size_t max_req);

void aio_destroy(struct aio_ctx *aio);

bool aio_full(struct aio_ctx *aio);

bool aio_empty(struct aio_ctx *aio);

size_t aio_pending(struct aio_ctx *aio);

void aio_prepare_read(struct aio_ctx *aio, int fd, void *buf, size_t count,
		      uint64_t off);

void aio_prepare_write(struct aio_ctx *aio, int fd, void *buf, size_t count,
		       uint64_t off);

void aio_fsync(struct aio_ctx *aio, int fd);

int aio_submit(struct aio_ctx *aio);

int aio_wait(struct aio_ctx *aio, uint64_t timeout_ms);

int file_open(const char *path, bool direct, bool trunc);

int file_read(int fd, void *buf, size_t n, uint64_t off);

int last_error(void);

int file_fdsync(int fd);

int file_close(int fd);

uint64_t file_size(int fd);

#endif // AIO_1732523983_H_
