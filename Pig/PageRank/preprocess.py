import sys

if __name__=="__main__":
    with open(sys.argv[1],'r') as f:
        lines = []
        linkDist = set()
        for line in f:
            links = line.split("\t")
            lines.append((links[0]+","+links[1]))
            for link in links:
                linkDist.add(link)
        idd = 0
        mapId = dict()
        f3 = open('./mapping.txt','w') 
        for link in linkDist:
            mapId[link] = idd
            f3.write(link+","+str(idd)+"\n")
            idd += 1
    f.close()
    f3.close()

    with open('./inputProcessed.txt','w') as f2:
        for line in lines:
            eachLink = line.split(",")
            oneData = ""
            for link in eachLink:
                oneData += str(mapId[link]) +","
            f2.write(oneData + "\n")
    f2.close()
        
            
