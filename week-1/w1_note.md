
# Week 1 - Part 4: Set up Google Cloud environment

+ Go to SSH directory  
``` cd ~/.ssh/```

+ Generate SSH key:  
```ssh-keygen -t rsa -f gcp -C nda -b 2048 ```

+ View public key:  
```cat gcp.pub```

+ Copy the public key into metadata of GCP Compute engine

+ Create a VM instance on GCP

+ ssh into VM **using your private key**:  
++ Method 1:  
```ssh -i ~/.ssh/gcp nda@<external ip address>```
++ Method 2:  
Create config file:  

        Host de-zoomcamp  
            HostName 34.142.161.225  
            User nda  
            IdentityFile ~/.ssh/gcp  

    ssh into vm: ```ssh de-zoomcamp```
+ View machine info:<br>
```htop```

+ Download and install anaconda for VM:  
```wget https://repo.anaconda.com/archive/Anaconda3-2022.10-Linux-x86_64.sh```  
```bash Anaconda3-2022-10-Linux-x86_64.sh```

