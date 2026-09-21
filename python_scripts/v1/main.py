import argparse
import sys
from utils.stearming import KafkaProducer, SamplePoller
import time

        
if __name__ == "__main__":
    argparser = argparse.ArgumentParser()
    argparser.add_argument("-b","--bootstrap.servers",type=str,default="localhost:9091",help=" an addres for bootstrap server as a whole string")
    argparser.add_argument("-t",'--topic',type=str,help="a topic to which the data should be send",required=True)
    argparser.add_argument("-p",'--path',type=str,help="path to the data source/file ",required=True)
    argparser.add_argument("-d","--degrade-factor",type=float,help=f"a number in range [0;1] which represent the % of which the sample field could be degraded eahc separetly , so the 50% means that each filed has 50% to become NULL/None",default=0)
    argparser.add_argument('-c',"--class.topic" ,type=str, help=f" name of the topic when the class topic will be send",required=True,default='target')

    args = argparser.parse_args()
    config= vars(args)
    
    poller = SamplePoller(config['path'],degrade_factor=config['degrade_factor'])
 
    print(config)
    prod= KafkaProducer(config={key:val for key,val in config.items() if key in ['topic','bootstrap.servers','class.topic']})
    prod.setPoller(poller)
   
    try:
        while True:
            prod.produce()
            time.sleep(5)
    except KeyboardInterrupt:
        print("the process was stopped with keybeoard inettrupt")
    except Exception as e :
        print('There as an exception\n ',e)
    finally:
        prod.producer.flush()
        del(poller)
        del(prod)
        sys.exit(1)
    
        
        
        


