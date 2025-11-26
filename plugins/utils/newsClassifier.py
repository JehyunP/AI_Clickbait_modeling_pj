from tensorflow.keras.models import load_model
import numpy as np

class NewsClassifier:
    def __init__(self):
        self.model = load_model("data/models/deep_learned_model.h5", compile=False)

    def round_probablity(self, prob, threshold=0.7):
        prob = float(prob)
        return 1 if prob >= threshold else 0

    
    def apply_model(self, data):
        # Extract title and content embedding
        title_emb = np.vstack(data["title_embedding"].values)
        content_emb = np.vstack(data["content_embedding"].values)

        pred = self.model.predict([title_emb, content_emb])

        data['evaluate_probability'] = pred.flatten()
        data['evaluate'] = [self.round_probablity(p) for p in pred.flatten()]
    
        return data