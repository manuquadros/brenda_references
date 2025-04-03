import json
from collections import defaultdict

import numpy as np
from hmmlearn import hmm
from sklearn.preprocessing import LabelEncoder


class EntityHMMClassifier:
    def __init__(self, n_components=None):
        """
        Initialize the HMM-based entity classifier
        n_components: Number of hidden states (will be set automatically if None)
        """
        self.char_encoder = LabelEncoder()
        self.state_encoder = LabelEncoder()
        self.n_components = n_components
        self.models = {}  # One HMM per class

    def _encode_sequence(self, text):
        """Convert text to sequence of integer indices"""
        return [self.char_vocab_[c] if c in self.char_vocab_ else -1 for c in text]

    def _prepare_sequence_data(self, terms):
        """Convert list of terms to format required by hmmlearn"""
        sequences = []
        lengths = []

        for term in terms:
            seq = self._encode_sequence(term)
            # Filter out unknown characters (marked as -1)
            seq = [s for s in seq if s != -1]
            if seq:  # Only add if sequence is not empty
                sequences.extend(seq)
                lengths.append(len(seq))

        return np.array(sequences).reshape(-1, 1), lengths

    def fit(self, training_data):
        """
        Train HMM models for each class
        training_data: List of dictionaries with 'term', 'class', and 'entid'
        """
        # Build character vocabulary
        all_chars = set()
        class_terms = defaultdict(list)
        self.class_entids = defaultdict(set)

        for entry in training_data:
            term = entry["term"]
            class_name = entry["class"]
            entid = str(entry["entid"])

            all_chars.update(term)
            class_terms[class_name].append(term)
            self.class_entids[class_name].add(entid)

        # Encode characters
        self.char_encoder.fit(list(all_chars))
        self.char_vocab_ = {c: i for i, c in enumerate(self.char_encoder.classes_)}
        n_chars = len(self.char_vocab_)

        # Train one HMM per class
        for class_name, terms in class_terms.items():
            # Determine number of components (states) if not specified
            if self.n_components is None:
                # Use sqrt of average term length as a heuristic
                avg_len = np.mean([len(term) for term in terms])
                n_components = max(2, int(np.sqrt(avg_len)))
            else:
                n_components = self.n_components

            # Initialize and train HMM
            model = hmm.MultinomialHMM(
                n_components=n_components, n_iter=100, random_state=42
            )

            # Prepare training data
            X, lengths = self._prepare_sequence_data(terms)

            if len(X) > 0:  # Only train if we have valid sequences
                model.n_features_ = n_chars
                model.fit(X, lengths=lengths)
                self.models[class_name] = model

    def predict(self, term):
        """
        Classify a term and return predicted class, entity ID, and score
        Returns: (predicted_class, possible_entids, score)
        """
        # Encode the sequence
        X, lengths = self._prepare_sequence_data([term])

        if len(X) == 0:  # If no valid characters
            return None, [], float("-inf")

        # Score with each model
        scores = {}
        for class_name, model in self.models.items():
            try:
                score = model.score(X, lengths)
                scores[class_name] = score
            except Exception:
                scores[class_name] = float("-inf")

        if not scores:
            return None, [], float("-inf")

        # Find best class
        best_class = max(scores.items(), key=lambda x: x[1])
        predicted_class = best_class[0]
        score = best_class[1]

        # Return possible entity IDs for this class
        possible_entids = list(self.class_entids[predicted_class])

        return predicted_class, possible_entids, score


# Example usage
def main() -> None:
    # Example training data
    training_data = [
        {"term": "Tyrosine translase", "class": "d3o:Enzyme", "entid": "3494"},
        {"term": "Tyrosyl-tRNA ligase", "class": "d3o:Enzyme", "entid": "3494"},
        {"term": "Escherichia coli", "class": "d3o:Bacteria", "entid": "2026"},
        {"term": "ATCC 35896", "class": "d3o:Strain", "entid": "16526"},
    ]

    # Initialize and train classifier
    classifier = EntityHMMClassifier()
    classifier.fit(training_data)

    # Test classification
    test_terms = ["Tyrosine kinase", "E. coli", "ATCC 12345", "E. tyrosine"]

    for term in test_terms:
        predicted_class, possible_entids, score = classifier.predict(term)
        print(f"\nTerm: {term}")
        print(f"Predicted class: {predicted_class}")
        print(f"Possible entity IDs: {possible_entids}")
        print(f"Log probability score: {score:.2f}")
