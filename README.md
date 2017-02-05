# Smart Broadcasting: Do you want to be seen?

This is the repository which contains code used for conducting experiments for the following paper:

> M. R. Karimi, E. Tavakoli, M. Farajtabar, L. Song, and M. Gomez-Rodriguez. Smart broadcasting: Do you want to be seen? In Proceedings of the 22nd ACM SIGKDD International Conference on Knowledge Discovery and Data Mining (KDD), 2016.

This is also used as a baseline for the paper:

> A. Zarezade, U. Upadhyay, H. R. Raibee, M. Gomez-Rodriguez. RedQueen: An Online Algorithm for Smart Broadcasting in Social Networks. In Proceedings of the 10th ACM International Conference on Web Search and Data Mining (WSDM), 2017.

## Important Dependencies

  - Python 3
  - `numpy`
  - `cvxopt`
  - `Cython`

## Installation

This package can be installed from the repository directly:

```
pip install git+https://github.com/Networks-Learning/broadcast_ref.git@master#egg=broadcast_ref
```

or, 

```
pip install git+https://github.com/Networks-Learning/broadcast_ref.git@py3#egg=broadcast_ref
```

After a successful installation, the module named `broadcast` should be available for import in a Python shell.


## Troubleshooting

If the `numpy` header files are not found while importing the `broadcast.opt.optimizer`, then:

  1. Find the `numpy` header files location:
       
       import numpy as np
       np.get_include()

  2. Launch your `jupyter` notebook or `python` shell after exporting `CFLAGS`:

       export CFLAGS="-I ${PATH_TO_NUMPY_INCLUDE} ${CFLAGS}"


