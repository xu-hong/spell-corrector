import requests
from pattern import web
import time
import sys, traceback


url = "http://en.wikipedia.org/wiki/Special:Random"
f = open("corpse.txt", "a")
try:
    while True:
        time.sleep(5) #delay for 5 seconds
        wiki = requests.get(url).text
        wiki = web.plaintext(wiki)
        print "writing a new article to file..."
        f.write(wiki.encode('utf8'))

except KeyboardInterrupt:
    print "Interrupted" 
except:
    traceback.print_exc(file=sys.stdout)
finally:
    f.flush()
    f.close()
    sys.exit(0)





