import numpy
from torch import Tensor

VALID_BATCH_FORMATS_NAMES = ["bytes", "tensor", "numpy", None]
VALID_BATCH_FORMATS = [bytes, Tensor, numpy.ndarray, None]