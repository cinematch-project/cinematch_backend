import time
import ast
import re
import pandas as pd
import numpy as np
import joblib
from pathlib import Path
from typing import Optional, List
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import linear_kernel
from sklearn.preprocessing import MinMaxScaler


class MovieRecommender:
    def __init__(self, df: Optional[pd.DataFrame] = None, csv_path: Optional[str] = None, engine=None):
        print("Initializing MovieRecommender...")
        start_load = time.time()
        if df is not None:
            self.df = df.copy()
        elif engine is not None:
            sql = "SELECT * FROM movie"
            self.df = pd.read_sql_query(sql, con=engine)
        elif csv_path is not None:
            self.df = pd.read_csv(csv_path)
        else:
            raise ValueError("Provide df, csv_path, or engine to MovieRecommender")

        print(f"Loaded data (in {time.time() - start_load:.2f}s) - rows: {len(self.df)}")
        self._prepare_data()
        self._calculate_popularity()
        self._create_soup()
        self._vectorize()

        self.id_to_index = pd.Series(self.df.index.values, index=self.df['id'].astype(str)).to_dict()
        print("MovieRecommender ready.")

    @staticmethod
    def _extract_names(x):
        if pd.isna(x) or x == '':
            return ''
        try:
            parsed = ast.literal_eval(x)
            if isinstance(parsed, list):
                if all(isinstance(elem, dict) for elem in parsed):
                    names = [d.get('name') for d in parsed if isinstance(d, dict) and 'name' in d]
                    return ' '.join([str(n) for n in names if n])
                return ' '.join([str(elem) for elem in parsed if elem])
            if isinstance(parsed, dict):
                return ' '.join([str(v) for v in parsed.values() if isinstance(v, (str, int))])
        except Exception:
            pass
        cleaned_str = re.sub(r'[\[\]{}"\\,\'`]', ' ', str(x))
        return cleaned_str

    def _prepare_data(self):
        self.df = self.df.drop_duplicates().reset_index(drop=True)
        keep_cols = ['id', 'tmdb_id', 'title', 'original_title', 'overview', 'genres', 'keywords', 'cast', 'crew',
                     'release_date', 'runtime', 'vote_average', 'vote_count', 'popularity', 'poster_path']
        for c in keep_cols:
            if c not in self.df.columns:
                self.df[c] = ''
        for c in ['title', 'overview', 'genres', 'keywords', 'cast', 'crew']:
            self.df[c] = self.df[c].fillna('').astype(str)
        for c in ['vote_average', 'vote_count', 'popularity', 'runtime']:
            self.df[c] = pd.to_numeric(self.df[c], errors='coerce').fillna(0)

    def _calculate_popularity(self):
        C = self.df['vote_average'].mean()
        m = self.df['vote_count'].quantile(0.95)
        self.C = C
        self.m = m
        self.df['weighted_rating'] = self.df.apply(self._weighted_rating, axis=1)
        scaler = MinMaxScaler()
        self.df['wr_normalized'] = scaler.fit_transform(self.df[['weighted_rating']])

    def _create_soup(self):
        for c in ['genres', 'keywords', 'cast', 'crew']:
            self.df[c + '_clean'] = self.df[c].apply(self._extract_names)
        self.df['soup'] = self.df.apply(lambda r: ' '.join([
            str(r.get(col, '')) for col in (
                'overview', 'genres_clean', 'keywords_clean', 'cast_clean', 'crew_clean'
            ) if r.get(col)
        ]), axis=1)

    def _vectorize(self):
        print("Starting TF-IDF vectorization (this may take a while)...")
        start_tfidf = time.time()
        tfidf = TfidfVectorizer(stop_words='english', max_features=50000)
        self.tfidf_matrix = tfidf.fit_transform(self.df['soup'].fillna(''))
        print(f" TF-IDF matrix created. Shape: {self.tfidf_matrix.shape} (In {time.time() - start_tfidf:.2f} sec)")

    def _weighted_rating(self, x):
        v = x['vote_count'] if 'vote_count' in x else 0
        R = x['vote_average'] if 'vote_average' in x else 0
        if v is None or R is None:
            return 0.0
        return (v / (v + self.m)) * R + (self.m / (v + self.m)) * self.C

    def _get_indices_from_ids(self, ids: List[int]) -> List[int]:
        """
        Convert a list of ids to DataFrame integer indices.
        Returns a list of unique indices in the same input order (deduped).
        """
        if not ids:
            return []
        indices = []
        seen = set()
        for raw in ids:
            if raw is None:
                continue
            key = str(raw)
            idx = self.id_to_index.get(key)
            if idx is not None and idx not in seen:
                indices.append(int(idx))
                seen.add(int(idx))
        return indices
    
    def save(self, path: str | Path):
        """
        Save the recommender's essential state (DataFrame, TF-IDF matrix, and parameters)
        to a .joblib file.
        """
        path = Path(path)
        state = {
            "df": self.df,
            "tfidf_matrix": self.tfidf_matrix,
            "C": self.C,
            "m": self.m,
        }
        joblib.dump(state, path)
        print(f"✅ Recommender saved to {path}")

    @classmethod
    def load(cls, path: str | Path) -> "MovieRecommender":
        """
        Load a recommender previously saved with .save().
        Returns a fully usable MovieRecommender instance.
        """
        path = Path(path)
        state = joblib.load(path)
        obj = cls.__new__(cls)  # bypass __init__
        obj.df = state["df"]
        obj.tfidf_matrix = state["tfidf_matrix"]
        obj.C = state["C"]
        obj.m = state["m"]

        # rebuild mappings for id/title lookup
        obj.tmdb_to_index = pd.Series(obj.df.index.values, index=obj.df["tmdb_id"].astype(str)).to_dict()
        obj.id_to_index = pd.Series(obj.df.index.values, index=obj.df["id"].astype(str)).to_dict()
        if "title" in obj.df.columns:
            obj.title_to_index = pd.Series(obj.df.index, index=obj.df["title"].str.lower()).to_dict()
        else:
            obj.title_to_index = {}

        print(f"✅ Recommender loaded from {path} (rows={len(obj.df)})")
        return obj

    def recommend(self, ids: List[int], top_n: int = 10, similarity_weight: float = 0.7):
        """
        Recommend movies based on a list of input IDs.
        Returns a pandas.DataFrame of recommended rows (top_n).
        """
        input_indices = self._get_indices_from_ids(ids)
        if not input_indices:
            return self.df.iloc[[]].copy()

        # build mean vector from input indices
        selected_vectors = self.tfidf_matrix[input_indices]
        mean_vector = selected_vectors.mean(axis=0)  # sparse matrix
        # convert to array for linear_kernel
        mean_vector_array = np.asarray(mean_vector)
        sim_scores = linear_kernel(mean_vector_array, self.tfidf_matrix).flatten()

        pop_scores = self.df['wr_normalized'].values
        if len(sim_scores) != len(pop_scores):
            pop_scores = np.zeros_like(sim_scores)

        popularity_weight = 1.0 - similarity_weight
        final_scores = (similarity_weight * sim_scores) + (popularity_weight * pop_scores)

        all_sorted_indices = final_scores.argsort()[::-1]
        recommended_indices = []
        input_set = set(input_indices)
        for idx in all_sorted_indices:
            if idx not in input_set:
                recommended_indices.append(int(idx))
            if len(recommended_indices) >= top_n:
                break

        return self.df.iloc[recommended_indices].copy()
