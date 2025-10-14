from client import MORK

authors_json = [
    {
        "Id": "author_001",
        "Name": "Leo Tolstoy",
        "Nationality": "Russian",
        "Birth": "1828-09-09",
        "Death": "1910-11-20",
        "Works": [
            {"Title": "War and Peace", "Published": 1869},
            {"Title": "Anna Karenina", "Published": 1877}
        ]
    },
    {
        "Id": "author_002",
        "Name": "Fyodor Dostoevsky",
        "Nationality": "Russian",
        "Birth": "1821-11-11",
        "Death": "1881-02-09",
        "Works": [
            {"Title": "Crime and Punishment", "Published": 1866}
        ]
    }
    ,
    {
        "Id": "author_003",
        "Name": "Jane Austen",
        "Nationality": "British",
        "Birth": "1775-12-16",
        "Death": "1817-07-18",
        "Works": [
            {"Title": "Pride and Prejudice", "Published": 1813},
            {"Title": "Emma", "Published": 1815}
        ]
    },
    {
        "Id": "author_004",
        "Name": "Gabriel García Márquez",
        "Nationality": "Colombian",
        "Birth": "1927-03-06",
        "Death": "2014-04-17",
        "Works": [
            {"Title": "One Hundred Years of Solitude", "Published": 1967},
            {"Title": "Love in the Time of Cholera", "Published": 1985}
        ]
    },
    {
        "Id": "author_005",
        "Name": "Chinua Achebe",
        "Nationality": "Nigerian",
        "Birth": "1930-11-16",
        "Death": "2013-03-21",
        "Works": [
            {"Title": "Things Fall Apart", "Published": 1958}
        ]
    },
    {
        "Id": "author_006",
        "Name": "Haruki Murakami",
        "Nationality": "Japanese",
        "Birth": "1949-01-12",
        "Death": "",
        "Works": [
            {"Title": "Norwegian Wood", "Published": 1987},
            {"Title": "Kafka on the Shore", "Published": 2002}
        ]
    }
]

def generate_and_run_queries(server, dataset):
    results = {"authors": [], "works": []}

    with server.work_at("authors") as scope:
        with scope.work_at("src") as src:
            for author in dataset:
                sexpr_author = f'''
                (Individuals author
                    (Id "{author["Id"]}")
                    (Name "{author["Name"]}")
                    (Nationality "{author["Nationality"]}")
                    (Birth "{author["Birth"]}")
                    (Death "{author["Death"]}")
                )
                '''
                # upload the s-expression into the src scope
                src.upload_(sexpr_author)
                results["authors"].append(author["Name"])

                for work in author.get("Works", []):
                    sexpr_work = f'''
                    (Works
                        (AuthorId "{author["Id"]}")
                        (Title "{work["Title"]}")
                        (Published "{work["Published"]}")
                    )
                    '''
                    # upload each work into the src scope
                    src.upload_(sexpr_work)
                    results["works"].append(work["Title"])

            # perform transforms from src -> simple
            t1 = scope.transform(
                ("(src (Individuals author (Id $id) (Name $name) (Nationality $nat) (Birth $birth) (Death $death)))",),
                ("(simple (Author $id $name $nat $birth $death))",)
            )
            # block until transform completes
            t1_meta = t1.block()

            t2 = scope.transform(
                ("(src (Works (AuthorId $id) (Title $title) (Published $year)))",),
                ("(simple (AuthorWork $id $title $year))",)
            )
            t2_meta = t2.block()

            # add an extra transform: group authors by nationality into 'nationality' space
            t3 = scope.transform(
                ("(simple (Author $id $name $nat $birth $death))",),
                ("(nationality (ByNationality $nat $id $name))",)
            )
            t3_meta = t3.block()

            # add another transform: index works by year into 'by_year' space
            t4 = scope.transform(
                ("(simple (AuthorWork $id $title $year))",),
                ("(by_year (WorkYear $year $id $title))",)
            )
            t4_meta = t4.block()

        with scope.work_at("simple") as simple:
            # download the transformed simple space contents
            simple_authors = simple.download("(Author $id $name $nat $birth $death)", "($id $name)")
            simple_works = simple.download("(AuthorWork $id $title $year)", "($id $title)")

            results["simple_authors"] = simple_authors.data
            results["simple_works"] = simple_works.data

        # download the new transformed spaces from their own scopes
        with scope.work_at("nationality") as nat_scope:
            nationality = nat_scope.download("(ByNationality $nat $id $name)", "($nat $id $name)")
            results["by_nationality"] = nationality.data

        with scope.work_at("by_year") as by_scope:
            by_year = by_scope.download("(WorkYear $year $id $title)", "($year $id $title)")
            results["by_year"] = by_year.data

    return results


def _main():
    server = MORK(base_url="http://127.0.0.1:8231")
    print("Connected to MORK server on 8231")

    parsed_results = generate_and_run_queries(server, authors_json)

    print("\nAuthors:", parsed_results["authors"], '\n')
    print("Their Works:", parsed_results["works"])
    print("\nTransform Authors:", parsed_results["simple_authors"])
    print("Transform Works:", parsed_results["simple_works"])
    # print additional transformed outputs
    print("\nBy Nationality:\n", parsed_results.get("by_nationality", ""))
    print("By Year:\n", parsed_results.get("by_year", ""))


if __name__ == "__main__":
    _main()