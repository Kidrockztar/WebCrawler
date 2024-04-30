import shelve

def generate_report(title, stats, file):
    f.write(f"----- {title} -----\n")
    if type(stats) == dict:
        for key, value in stats.items():
            f.write(f"{key} {value}\n")
    elif type(stats) == list:
        for value in stats:
            f.write(f"{value}\n")
    elif type(stats) == int:
        f.write(f"{stats}\n")
    f.write("------------------\n")


if __name__ == "__main__":
    with open("report.txt", "w") as f:

        tokens = shelve.open("tokens.shelve")
        urlCount = shelve.open("uniquePages.shelve")
        subDomains = shelve.open("subDomains.shelve")
        longest = shelve.open("longest.shelve")
        robot = shelve.open("robotTXTs.shelve")
    
        generate_report("Number of unique pages ", len(urlCount),f)

        generate_report("Number of tokens", len(tokens), f)
        generate_report("Longest page", list(longest.items()), f)
        
        sorted_dict = list(sorted(tokens.items(), key=lambda item: (-item[1], item[0])))
        generate_report("Top 50 tokens", sorted_dict[:50], f)

        sorted_URL_subdomains = dict(sorted(subDomains.items(), key=lambda item: (-item[1], item[0])))
       
        generate_report("Top subdomains", sorted_URL_subdomains, f)

        generate_report("Lenght of robotTXTs", len(robot), f)

    with open("uniquepages.txt", "w") as f:
        generate_report("Number of pages", len(urlCount.items()), f)
        results = dict(urlCount.items())
        generate_report("Longest page", results, f)
        