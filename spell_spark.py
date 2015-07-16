import re, collections
from pyspark import SparkContext

def edits1(word):
    splits = [(word[:i], word[i:]) for i in range(len(word)+1)]
    deletes = [a + b[1:] for a, b in splits if b]
    transposes = [a + b[1] + b[0] + b[2:] for a, b in splits if len(b) > 1]
    replaces = [a + c + b[1:] for a, b in splits for c in alphabet if b]
    inserts = [a + c + b for a, b in splits for c in alphabet]
    return set(deletes + transposes + replaces + inserts)

def known_edits2(word):
    return set(e2 for e1 in edits1(word) for e2 in edits1(e1) if e2 in NWORDS)

def known(words): return set(w for w in words if w in NWORDS)

def correct(word):
    candidates = known([word]) or known(edits1(word)) or known_edits2(word) or [word]
    return max(candidates, key=NWORDS.get)


################# test code start from here ####################
def spelltest(tests, bias=None, verbose=False):
    import time
    n, bad, unknown, start = 0, 0, 0, time.clock()
    if bias:
        for target in tests: NWORDS[target] += bias
    for target,wrongs in tests.items():
        for wrong in wrongs.split():
            n += 1
            w = correct(wrong)
            if w!=target:
                bad += 1
                unknown += (target not in NWORDS)
                if verbose:
                    print 'correct(%r) => %r (%d); expected %r (%d)' % (
                        wrong, w, NWORDS[w], target, NWORDS[target])
    return dict(bad=bad, n=n, bias=bias, pct=int(100. - 100.*bad/n), 
                unknown=unknown, secs=int(time.clock()-start) )


if __name__ == '__main__':

    sc = SparkContext(appName="spell_corrector")

    corpseRDD = sc.textFile("code/spell/corpse.text", 4)

    wordsRDD = corpseRDD.flatMap(lambda line: re.findall(r'[a-z]+', line.lower()))
    countwordsRDD = wordsRDD.map(lambda x: (x, 1)).reduceByKey(lambda x, y: x + y)

    countwords = countwordsRDD.collect()

    NWORDS = collections.defaultdict(lambda: 1)
    NWORDS.update(dict(countwords))
    
    #NWORDS = sc.broadcast(NWORDS)
    #print type(NWORDS)

    alphabet = 'abcdefghijklmnopqrstuvwxyz'

    print spelltest(tests1)
    
    sc.stop()
