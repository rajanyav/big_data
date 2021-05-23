import glob
import matplotlib.pyplot as plt
import numpy as np

def mean(tl):
    return sum(tl) / len(tl)

def median(tl):
    n = len(tl)
    tl.sort()

    if n % 2 == 0:
        m1 = tl[n//2]
        m2 = tl[n//2 - 1]
        return (m1 + m2)/2
    else:
        return tl[n//2]

if __name__ == "__main__":
    # JOb completion times mean and median
    with open('master.log', 'r') as file:
        tct = []  #task completion time
        while True:
            #extract times of each job stored in a separate line
            line = file.readline()
            if not line:
                break
            tct.append(float(line.split(':')[-1]))

        print('mean(jobs) = ', str(mean(tct)))
        print('median(jobs) = ', str(median(tct)))

    # Task completion times mean and meadian
    files = glob.glob('worker*.log')
    tct = []
    num_tasks = []
    etimes = [] #execution times
    tasks = []
    s = 1
    for f in files:
        c, t = 0, 0
        with open(f, 'r') as file:
            while True:
                #extract times of each job stored in a separate line
                line = file.readline()
                if not line:
                    break
                tt = float(line.split(':')[-1])
                c = c + 1
                t = t + tt
                tct.append(tt)
            num_tasks.append(c)
            etimes.append(t)
        tasks.append('W' + str(s) + '(' + str(c) + ')')
        s = s + 1


    print('mean(tasks) = ', str(mean(tct)))
    print('median(tasks) = ', str(median(tct)))

    # creating the bar plot
    fig = plt.figure()
    plt.bar(tasks, etimes, color ='maroon', width = 0.4)
    plt.ylabel("Time")
    plt.xlabel("No. of num_tasks scheduled")
    plt.title("Number of num_tasks scheduled on each machine against time")
    plt.show()
