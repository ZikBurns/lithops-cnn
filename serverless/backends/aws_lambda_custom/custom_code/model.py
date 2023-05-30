import base64
import io

import numpy
from torch import Tensor
from transformations import apply_tfms,normalize_batch
from torchvision import transforms
from PIL import Image
import torch
from torch.utils.data.dataloader import default_collate

class OffSamplePredictModel:
    def __init__(self, model_path):
        self.jit_model = torch.jit.load(model_path,torch.device('cpu'))
        self.target_class_idx = 0

    def predict(self, images_data):
        labels = []
        probabilities = []
        xtensors = []
        for image_data in images_data:
            image = Image.open(io.BytesIO(image_data)).convert('RGB')
            # PIL Image to Fastai Image
            imgtensor = transforms.ToTensor()(image)
            x = apply_tfms(imgtensor, size=224, padding_mode='reflection', mode='bilinear')
            xtensors.append(x.data)

        batch = default_collate(xtensors)
        batch_normed = normalize_batch([batch, Tensor([0, 0])], mean=Tensor([0.485, 0.456, 0.406]),
                                       std=Tensor([0.229, 0.224, 0.225]))

        # resnet.eval()
        with torch.no_grad():
            out = self.jit_model.forward(batch_normed[0])
        out = torch.softmax(out, dim=1)
        pred_probs = out.numpy()
        preds = pred_probs.argmax(axis=1)  # _, preds = torch.max(out.data, 1)
        for i in range(len(pred_probs)):
            probabilities.append(pred_probs[i][0])
            if (preds[i] == 0):
                labels.append('off')
            else:
                labels.append('on')

        return probabilities, labels