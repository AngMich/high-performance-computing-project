from pyspark import SparkContext
import time
def split_file(line):
    return line.strip().split()

def sum_op(x, y):
    return x + y


def reduced_candidate_list(pair):
    candidate_itemset = list(pair[0])

    unique_items = pair[1]

    if unique_items not in candidate_itemset:
        candidate_itemset.append(unique_items)
        candidate_itemset.sort()

    return tuple(candidate_itemset)


if __name__ == "__main__":

    cores_str = input("Enter the number of cores to use (0 for all cores): ")
    try:
        cores = int(cores_str)
    except ValueError:
        cores = 0

    if cores <= 0:
        threads = "local[*]"
    else:
        threads = f"local[{cores}]"

    print(f"Using Spark master = {threads}")

    sc = SparkContext(threads, "B1")

    transactions = sc.textFile("transactions.txt").map(split_file).cache()

    minCount = 2

    start_time = time.time()

    list_items = transactions.flatMap(lambda tx: tx)

    one_item_count = (
        list_items.map(lambda it: (it, 1))
               .reduceByKey(sum_op)
               .filter(lambda x: x[1] >= minCount)
    )

    freq_itemsets = one_item_count.map(lambda x: ((x[0],), x[1]))
    current_size = one_item_count.map(lambda x: (x[0],))

    unique_items = list_items.distinct().cache()

    k = 2
    while not current_size.isEmpty() and k < 4:
        candidates = (
            current_size.cartesian(unique_items)
                         .map(reduced_candidate_list)
                         .filter(lambda t: len(t) == k)
                         .distinct()
        )
        if candidates.isEmpty():
            break
        cand_tx = candidates.cartesian(transactions)
        cand_tx_filtered = cand_tx.filter(
            lambda ct: all(x in ct[1] for x in ct[0])
        )
        cand_counts = (
            cand_tx_filtered.map(lambda ct: (ct[0], 1))
                            .reduceByKey(sum_op)
                            .filter(lambda x: x[1] >= minCount)
        )
        if cand_counts.isEmpty():
            break
        freq_itemsets = freq_itemsets.union(cand_counts)
        current_size = cand_counts.map(lambda x: x[0])
        k += 1
    end_time = time.time()
    wallclock = end_time - start_time

    print("\nTotal Apriori time:", wallclock, "seconds")
    print("\nFrequent itemsets (itemset => count):")
    for itemset, count in freq_itemsets.collect():
        print(list(itemset), "=>", count)

    sc.stop()
