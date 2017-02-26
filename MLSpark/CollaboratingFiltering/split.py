import sys
import re
if __name__=="__main__":
    with open('./converted.txt', 'w') as f1:
        for line in open(sys.argv[1], 'r'):
            flag = True
            intFlag = True
            #print line
            for word in line.split(","):
                #print word
                if not re.match("^[0-9]*$",word):
                    flag = False
                else:
                    try : 
                        int(word)
                    except ValueError:
                        intFlag = False
            if (flag) and (intFlag):
                f1.write(line)

    
            
