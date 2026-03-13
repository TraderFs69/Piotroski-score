from concurrent.futures import ThreadPoolExecutor

def run_parallel(func, items, max_workers=25):

    results = []

    with ThreadPoolExecutor(max_workers=max_workers) as executor:

        futures = [executor.submit(func, item) for item in items]

        for f in futures:
            try:
                r = f.result()
                if r:
                    results.append(r)
            except:
                pass

    return results
