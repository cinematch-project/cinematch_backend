from pathlib import Path
import pandas as pd
import numpy as np
import joblib
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
from sklearn.preprocessing import MinMaxScaler

from app.models.dto import RecommenderMovie


class MovieRecommender:
    default_weights = {"overview": 0.2, "genres": 0.4, "keywords": 0.4}

    def __init__(self, engine):
        print("Creating MovieRecommender...")
        self.df = None
        self.matrices = {}
        self.vectorizers = {}
        self.scaler = MinMaxScaler()
        self.C = None
        self.m = None

        if engine:
            print("Found engine")
            self._load_data(engine)
            self._calculate_popularity()
            self._build_models()
            print("✅ Recommender ready")
        else:
            raise Exception("No database engine provided.")

    def _load_data(self, engine):
        print("Loading data from engine...", end=" ", flush=True)
        # Added vote_average and vote_count for popularity calc
        query = """
            SELECT
                m.id,
                m.title,
                m.overview,
                m.vote_average,
                m.vote_count,
                (SELECT GROUP_CONCAT(g.name, ', ')
                 FROM genre g
                 JOIN moviegenrelink l ON l.genre_id = g.id
                 WHERE l.movie_id = m.id) AS genres,
                (SELECT GROUP_CONCAT(k.name, ', ')
                 FROM keyword k
                 JOIN moviekeywordlink l ON l.keyword_id = k.id
                 WHERE l.movie_id = m.id) AS keywords
            FROM movie m
        """
        self.df = pd.read_sql(query, engine)

        # Clean data
        self.df["overview"] = self.df["overview"].fillna("")
        self.df["genres"] = self.df["genres"].fillna("")
        self.df["keywords"] = self.df["keywords"].fillna("")
        print("✅")

    def _calculate_popularity(self):
        """
        Calculates IMDB-style Weighted Rating and normalizes it 0-1.
        """
        print("Calculating IMDB-style Weighted Rating...", end=" ", flush=True)
        # C is the mean vote across the whole report
        self.C = self.df["vote_average"].mean()
        # m is the minimum votes required to be listed (95th percentile)
        self.m = self.df["vote_count"].quantile(0.95)

        # Calculate Weighted Rating
        self.df["weighted_rating"] = self.df.apply(self._weighted_rating, axis=1)

        # Normalize to 0-1 scale so it matches Cosine Similarity scale
        self.df["wr_normalized"] = self.scaler.fit_transform(
            self.df[["weighted_rating"]]
        )
        print("✅")

    def _weighted_rating(self, x):
        v = x["vote_count"]
        R = x["vote_average"]
        # Avoid division by zero or empty data
        if v == 0:
            return 0
        return (v / (v + self.m) * R) + (self.m / (v + self.m) * self.C)

    def _tokenize_tags(self, text):
        """
        Turns 'Science Fiction, New York' into 'Science_Fiction New_York'.
        This preserves multi-word tags without needing prefixes.
        """
        if not text:
            return ""
        # Split by comma, strip whitespace, replace internal spaces with underscore
        tags = [t.strip().replace(" ", "_").lower() for t in text.split(",")]
        return " ".join(tags)

    def _build_models(self):
        print("Building Multi-Vector TF-IDF models...", end=" ", flush=True)

        # 1. Overview Vectorizer (Standard English text)
        self.vectorizers["overview"] = TfidfVectorizer(
            stop_words="english", max_features=30000
        )
        self.matrices["overview"] = self.vectorizers["overview"].fit_transform(
            self.df["overview"]
        )

        # 2. Genres Vectorizer (Tags)
        # Pre-process to lock multi-word genres
        genre_soup = self.df["genres"].apply(self._tokenize_tags)
        self.vectorizers["genres"] = TfidfVectorizer(
            token_pattern=r"(?u)\b[\w-]+\b", max_features=30000
        )
        self.matrices["genres"] = self.vectorizers["genres"].fit_transform(genre_soup)

        # 3. Keywords Vectorizer (Tags)
        keyword_soup = self.df["keywords"].apply(self._tokenize_tags)
        self.vectorizers["keywords"] = TfidfVectorizer(
            token_pattern=r"(?u)\b[\w-]+\b", max_features=30000
        )
        self.matrices["keywords"] = self.vectorizers["keywords"].fit_transform(
            keyword_soup
        )

        print("✅")

    def recommend(
        self, movie_ids, top_n=20, min_score=0.1, similarity_weight=0.8, weights=None
    ) -> list[RecommenderMovie]:
        """
        Complex recommendation combining 3 content vectors + popularity.

        Args:
            weights (dict): Weights for overview, genres, keywords (sum should ideally be 1.0)
            similarity_weight (float): Balance between Content (0.8) and Popularity (0.2)
        """
        if self.df is None:
            raise Exception("Model not loaded.")

        if weights is None:
            weights = self.default_weights

        popularity_weight = 1.0 - similarity_weight

        # 1. Get Input Indices
        input_indices = self.df[self.df["id"].isin(movie_ids)].index.tolist()
        if not input_indices:
            return []

        # 2. Calculate Cosine Similarity for each column independently
        # We calculate the mean vector of the input movies for each category
        sim_scores = {}

        for col in ["overview", "genres", "keywords"]:
            matrix = self.matrices[col]
            # Get vectors for input movies
            input_vectors = matrix[input_indices]
            # Average them to get user profile
            user_profile = np.asarray(np.mean(input_vectors, axis=0))
            # Calculate cosine similarity (flatten to 1D array)
            sim_scores[col] = cosine_similarity(user_profile, matrix).flatten()

        # 3. Get Popularity Scores
        pop_scores = self.df["wr_normalized"].values

        # 4. Combine Scores & Calculate Contributions
        results = []

        # Iterate through all movies to calculate final weighted score
        # Note: Vectorizing this math is faster, but loop is clearer for contribution logic

        total_content_scores = (
            sim_scores["overview"] * weights.get("overview", 0)
            + sim_scores["genres"] * weights.get("genres", 0)
            + sim_scores["keywords"] * weights.get("keywords", 0)
        )

        final_scores = (total_content_scores * similarity_weight) + (
            pop_scores * popularity_weight
        )

        # Create candidate list
        candidates = []
        for idx, score in enumerate(final_scores):
            if idx in input_indices:
                continue  # Skip inputs
            if score < min_score:
                continue

            candidates.append((idx, score))

        # Sort by final score
        candidates = sorted(candidates, key=lambda x: x[1], reverse=True)[:top_n]

        # 5. Format Output with Contribution percentages
        for idx, final_score in candidates:

            # Retrieve raw similarity components
            s_ov = sim_scores["overview"][idx]
            s_ge = sim_scores["genres"][idx]
            s_kw = sim_scores["keywords"][idx]
            s_pop = pop_scores[idx]

            # Calculate weighted contribution parts
            c_ov = s_ov * weights.get("overview", 0) * similarity_weight
            c_ge = s_ge * weights.get("genres", 0) * similarity_weight
            c_kw = s_kw * weights.get("keywords", 0) * similarity_weight
            c_pop = s_pop * popularity_weight

            # Avoid division by zero
            safe_divisor = final_score if final_score > 0 else 1.0

            contributions = {
                "overview": round((c_ov / safe_divisor) * 100, 2),
                "genres": round((c_ge / safe_divisor) * 100, 2),
                "keywords": round((c_kw / safe_divisor) * 100, 2),
                "popularity": round((c_pop / safe_divisor) * 100, 2),
            }

            results.append(
                {
                    "id": int(self.df.iloc[idx]["id"]),
                    "relevance_score": round(float(final_score), 4),
                    "column_contribution": contributions,
                }
            )

        return results

    def save(self, path: str | Path):
        model_data = {
            "df": self.df,
            "matrices": self.matrices,
            "vectorizers": self.vectorizers,
            "scaler": self.scaler,
            "C": self.C,
            "m": self.m,
        }
        path = Path(path)
        joblib.dump(model_data, path)
        print(f"✅ Recommender saved to {path}")

    @classmethod
    def load(cls, path: str | Path):
        path = Path(path)
        print(f"Loading model from {path}...")
        data = joblib.load(path)
        rec = cls.__new__(cls)  # bypass __init__
        rec.df = data["df"]
        rec.matrices = data["matrices"]
        rec.vectorizers = data["vectorizers"]
        rec.scaler = data["scaler"]
        rec.C = data["C"]
        rec.m = data["m"]
        print(f"✅ Recommender loaded from {path} (rows={len(rec.df)})")
        return rec
