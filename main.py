from elementaree import Elementaree
from chefmarket import Chefmarket
from threading import Thread

if __name__ == '__main__':

    t1 = Thread(target=Elementaree().parse)
    t2 = Thread(target=Chefmarket().parse)

    t1.start()
    t2.start()
