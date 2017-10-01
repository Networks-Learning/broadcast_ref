CFLAGS="-I \"$(python -c 'import numpy;print(numpy.get_include())')\" ${CFLAGS}" python -c 'import broadcast.opt.optimizer'


