from torch.jit import RecursiveScriptModule
from typing import List
from torch import Tensor
import torch
from torch.utils.data.dataloader import default_collate
from lithops.serverless.backends.aws_lambda_custom.custom_code.transformations import normalize_batch

class OffSamplePredictModel:
    def __init__(self, model_path):
        self.jit_model = torch.jit.load(model_path,torch.device('cpu'))

    def predict(self, tensors):
        labels = []
        probabilities = []

        batch = default_collate(tensors)
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


class ModelEnsemble(torch.nn.Module):
    def __init__(self, model, n_models):
        super().__init__()
        self.n_models = n_models
        self.models = torch.nn.ModuleList([model for _ in range(self.n_models)])

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        # Divide in segments
        segment_size = x.shape[0] // self.n_models

        #Calculate remainder and distribute it
        remainder = x.shape[0] % self.n_models
        if remainder>0:
            segment_size=segment_size+1

        # Split the tensor into segment tensors
        segments = torch.split(x, segment_size, dim=0)

        #Check status
        # print(f"Batch size: {x.shape[0]}")
        # print(f"Number of models: {self.n_models}")
        # print(f"Segment size: {segment_size}")
        # print(f"Remainder: {remainder}")
        # print(f"Number of segments: {len(segments)}")

        # Launch tasks for each model and segment tensor
        futures: List[torch.jit.Future[torch.Tensor]] = []
        for i, model in enumerate(self.models):
            if (self.n_models == len(segments) or (self.n_models > len(segments) and i < len(segments))):
                segment = segments[i]
                future = torch.jit.fork(model, segment)
                futures.append(future)

        # Collect futures
        results: List[torch.Tensor] = []
        for future in futures:
            result = torch.jit.wait(future)
            results.append(result)

        # Cat the results
        results = torch.cat(results, dim=0)

        return results

class OffSampleTorchscriptFork:
    def __init__(self, ens: RecursiveScriptModule):
        self.ens = ens

    def predict(self, tensors):
        labels = []
        probabilities = []

        batch = default_collate(tensors)
        batch_normed = normalize_batch([batch, Tensor([0, 0])], mean=Tensor([0.485, 0.456, 0.406]),
                                       std=Tensor([0.229, 0.224, 0.225]))[0]

        with torch.no_grad():
            out = self.ens(batch_normed)

        out = torch.softmax(out, dim=1)
        pred_probs = out.numpy()
        preds = pred_probs.argmax(axis=1)  # _, preds = torch.max(out.data, 1)
        for i in range(len(pred_probs)):
            probabilities.append(pred_probs[i][0])
            if (preds[i] == 0):
                labels.append('off')
            else:
                labels.append('on')
        pred_list = [{'prob': float(prob), 'label': label} for prob, label in zip(probabilities, labels)]

        return pred_list


