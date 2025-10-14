import unittest
from generate_and_run_queries import generate_and_run_queries, authors_json
from client import MORK

class TestAuthorGenerator(unittest.TestCase):
    def test_author_names(self):
        server = MORK(base_url="http://127.0.0.1:8231")
        result = generate_and_run_queries(server, authors_json)
        self.assertIn("Leo Tolstoy", result["authors"])
        self.assertIn("Crime and Punishment", result["works"])
        self.assertIn("Russian", result.get("by_nationality"))

if __name__ == "__main__":
    unittest.main()
