conda create -n tweet python=3.8 -y
conda activate tweet
pip install git+https://github.com/tweepy/tweepy.git
pip install matplotlib seaborn numpy pandas wordcloud tqdm
conda install ipykernel -y