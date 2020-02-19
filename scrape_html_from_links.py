import urllib.request
import re
import io


def get_entry(url):
    re_tag = re.compile("<.+?>")
    
    req = urllib.request.Request(url, headers={'User-Agent' : "Magic Browser"}) 
    con = urllib.request.urlopen(req)
    
    entry = "See Bulbapedia page for details on this Pokémon."
    line = con.readline().decode("utf-8")
    while line != "":
        if line.startswith('<li> <a href="http://archives.bulbagarden.net'):
            line = con.readline().decode("utf-8")
            entry = ""
            while line != "" and not line.startswith('<div id="toc" class="toc">'):
                txt = re.sub(re_tag, '', line.strip().replace('</p>', '\n\n').replace('</li>', '\n').replace('</ul>', '\n'))
                entry += txt
                line = str(con.readline().decode("utf-8"))
            break
        line = con.readline().decode("utf-8")
    entry = entry.strip()

    print(entry)
    
    return entry


iFile = 'links.txt'
oFile = 'entries.txt'

i_fp = open(iFile, 'r')
o_fp = io.open(oFile, 'w', encoding="utf-8")

i_line = i_fp.readline().strip()
while i_line != "":
    o_line = ""
    if i_line != "#VALUE!":
        o_line = get_entry(i_line)
    o_fp.write(o_line + '\t')
    i_line = i_fp.readline().strip()

i_fp.close()
o_fp.close()
