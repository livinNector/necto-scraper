import yarl
def make_absolute_url(url,base):
    if yarl.URL(url).is_absolute():
        return url
    else:
        return yarl.URL(base).join(url)
