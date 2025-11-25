import re
from sentence_transformers import SentenceTransformer
import pandas as pd


class NewsEmbedding:
    def __init__(self):
        self.model = SentenceTransformer("snunlp/KR-SBERT-V40K-klueNLI-augSTS")

    def clean_text(self, text):
        if not text:
            return ""
        if isinstance(text, list):
            text = " ".join(str(item) for item in text if item)
        text = str(text)
        text = re.sub(r'<[^>]+>', '', text)
        text = re.sub(r'[!?]{2,}', '!', text)
        text = re.sub(r'\.{2,}', '...', text)
        text = re.sub(r'\s+', ' ', text)
        text = text.strip()
        return text

    def embed(self, title, content):
        # 텍스트 정제
        clean_title = self.clean_text(title)
        clean_content = self.clean_text(content)

        # 빈 텍스트 처리
        if not clean_title:
            clean_title = "제목 없음"
        if not clean_content:
            clean_content = "내용 없음"

        embeddings = self.model.encode(
            [clean_title, clean_content],
            batch_size=64,
            convert_to_numpy=True,
            normalize_embeddings=True
        )

        return embeddings[0], embeddings[1] # (768,), (768,) <- Shape



def process_json(data_list):
    """
    JSON 파일을 읽어 임베딩 생성

    Args:
        data_list: JSON 파일
    """

    df = pd.DataFrame(data_list)

    # 임베딩 생성
    embedder = NewsEmbedding()

    title_embeddings = []
    content_embeddings = []

    for idx, row in df.iterrows():
        title = row["Title"]       
        content = row["Contents"]  

        t_emb, c_emb = embedder.embed(title, content)

        title_embeddings.append(t_emb)
        content_embeddings.append(c_emb)

    df['title_embedding'] = title_embeddings
    df['content_embedding'] = content_embeddings


    return df


