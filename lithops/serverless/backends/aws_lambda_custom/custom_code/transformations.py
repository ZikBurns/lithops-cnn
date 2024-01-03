import io

import torch.nn.functional as F
from torch import Tensor
import torch
from PIL import Image
from torchvision import transforms

TensorImageSize = (int,int,int)
FloatTensor = torch.FloatTensor

class FlowField():
    "Wrap together some coords `flow` with a `size`."
    def __init__(self, size: (int, int), flow: Tensor):
        self.size = size
        self.flow = flow

def _grid_sample(x:Tensor, coords:FlowField, mode:str='bilinear', padding_mode:str='reflection')->Tensor:
    "Resample pixels in `coords` from `x` by `mode`, with `padding_mode` in ('reflection','border','zeros')."
    coords = coords.flow.permute(0, 3, 1, 2).contiguous().permute(0, 2, 3, 1) # optimize layout for grid_sample
    if mode=='bilinear': # hack to get smoother downwards resampling
        mn,mx = coords.min(),coords.max()
        # max amount we're affine zooming by (>1 means zooming in)
        z = 1/(mx-mn).item()*2
        # amount we're resizing by, with 100% extra margin
        d = min(x.shape[1]/coords.shape[1], x.shape[2]/coords.shape[2])/2
        # If we're resizing up by >200%, and we're zooming less than that, interpolate first
        if d>1 and d>z: x = F.interpolate(x[None], scale_factor=1/d, mode='area')[0]
    return F.grid_sample(x[None], coords, mode=mode, padding_mode=padding_mode,align_corners=True)[0]


def _affine_grid(size:TensorImageSize)->FlowField:
    size = ((1,)+size)
    N, C, H, W = size
    grid = FloatTensor(N, H, W, 2)
    linear_points = torch.linspace(-1, 1, W) if W > 1 else Tensor([-1])
    grid[:, :, :, 0] = torch.ger(torch.ones(H), linear_points).expand_as(grid[:, :, :, 0])
    linear_points = torch.linspace(-1, 1, H) if H > 1 else Tensor([-1])
    grid[:, :, :, 1] = torch.ger(linear_points, torch.ones(W)).expand_as(grid[:, :, :, 1])
    return FlowField(size[2:], grid)

def affine_grid_sample(img: Tensor , size: int = None, padding_mode: str = 'reflection', mode: str = 'bilinear') -> Tensor:
    "Apply all `tfms` to the `Image`, with resize_method=ResizeMethod.SQUISH"
    if not (size) or size is None: return img
    crop_target = (size,size)
    flow = _affine_grid((img.shape[0],) + crop_target)
    img = _grid_sample(img,flow, padding_mode=padding_mode, mode=mode)
    return img

def normalize(x:Tensor, mean:FloatTensor,std:FloatTensor)->Tensor:
    "Normalize `x` with `mean` and `std`."
    return (x-mean[...,None,None]) / std[...,None,None]

def normalize_batch(b:(Tensor,Tensor), mean:FloatTensor, std:FloatTensor, do_x:bool=True, do_y:bool=False)->(Tensor,Tensor):
    "`b` = `x`,`y` - normalize `x` array of imgs and `do_y` optionally `y`."
    x,y = b
    mean,std = mean.to(x.device),std.to(x.device)
    if do_x: x = normalize(x,mean,std)
    if do_y and len(y.shape) == 4: y = normalize(y,mean,std)
    return x,y

def transform_images(images_data):
    xtensors = []
    for image_data in images_data:
        image = Image.open(io.BytesIO(image_data)).convert('RGB')
        # PIL Image to Fastai Image
        imgtensor = transforms.ToTensor()(image)
        x = affine_grid_sample(imgtensor, size=224, padding_mode='reflection', mode='bilinear')
        xtensors.append(x.data)
    return xtensors



def transform_image(image_data):
    image_transforms = transforms.Compose([
        transforms.ToTensor(),
        CustomAffineGridSample(224),
    ])
    image = Image.open(io.BytesIO(image_data)).convert('RGB')
    tensor = image_transforms(image)
    return tensor

class CustomAffineGridSample:
    def __init__(self, size:int =224, padding_mode:str='reflection', mode:str='bilinear' ):
        self.size = size
        self.padding_mode = padding_mode
        self.mode = mode

    def _grid_sample(self,x: Tensor, coords: FlowField, mode: str = 'bilinear', padding_mode: str = 'reflection') -> Tensor:
        "Resample pixels in `coords` from `x` by `mode`, with `padding_mode` in ('reflection','border','zeros')."
        coords = coords.flow.permute(0, 3, 1, 2).contiguous().permute(0, 2, 3, 1)  # optimize layout for grid_sample
        if mode == 'bilinear':  # hack to get smoother downwards resampling
            mn, mx = coords.min(), coords.max()
            # max amount we're affine zooming by (>1 means zooming in)
            z = 1 / (mx - mn).item() * 2
            # amount we're resizing by, with 100% extra margin
            d = min(x.shape[1] / coords.shape[1], x.shape[2] / coords.shape[2]) / 2
            # If we're resizing up by >200%, and we're zooming less than that, interpolate first
            if d > 1 and d > z: x = F.interpolate(x[None], scale_factor=1 / d, mode='area')[0]
        return F.grid_sample(x[None], coords, mode=mode, padding_mode=padding_mode, align_corners=True)[0]

    def _affine_grid(self,size: TensorImageSize) -> FlowField:
        size = ((1,) + size)
        N, C, H, W = size
        grid = FloatTensor(N, H, W, 2)
        linear_points = torch.linspace(-1, 1, W) if W > 1 else Tensor([-1])
        grid[:, :, :, 0] = torch.ger(torch.ones(H), linear_points).expand_as(grid[:, :, :, 0])
        linear_points = torch.linspace(-1, 1, H) if H > 1 else Tensor([-1])
        grid[:, :, :, 1] = torch.ger(linear_points, torch.ones(W)).expand_as(grid[:, :, :, 1])
        return FlowField(size[2:], grid)

    def __call__(self, img:Tensor):
        if not (self.size) or self.size is None: return img
        crop_target = (self.size, self.size)
        flow = self._affine_grid((img.shape[0],) + crop_target)
        img = self._grid_sample(img, flow, padding_mode=self.padding_mode, mode=self.mode)
        return img