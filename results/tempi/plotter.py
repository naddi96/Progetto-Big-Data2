
from statistics import mean
import matplotlib.pyplot as plt
import glob

def plot(filename,list):
    fig = plt.figure()
    fig.suptitle(filename, fontsize=14, fontweight='bold')

    ax = fig.add_subplot(111)
    ax.set_xlabel('messaggio')
    ax.set_ylabel('latenza in secondi')
    plt.plot(list)
    plt.savefig(filename+".png")
    

def sta(filename):
    f= open(filename,"r")
    total=0
    listOld=[]
    listNew=[]
    while True:
        line = f.readline()
        if not line: 
            break
    
        line = line.split(",")
        try:
            listOld.append(float(line[1]))
            listNew.append(float(line[0]))
        except:
            continue

    plot("old-latency-"+filename,listOld)
    plot("new-latency-"+filename,listNew)
    print(filename,"old latency",mean(listOld),min(listOld),max(listOld))
    print(filename,"new latency",mean(listNew),min(listNew),max(listNew))


    return [listNew,listOld]

dic={}
for file in glob.glob("*.csv"):
    dic[file]=sta(file)


