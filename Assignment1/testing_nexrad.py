def file_URL(filename):
    x=filename.split("_")
    stat=x[0][:4]
    y=x[0][4:8]
    m=x[0][8:10]
    d=x[0][10:12]
    hh=x[1][:2]
    mm=x[1][2:4]
    ss=x[1][4:6]
    ext=x[-1][-3:]
    return "https://noaa-nexrad-level2.s3.amazonaws.com/"+y+"/"+m+"/"+d+"/"+stat+"/"+filename

def test_01():
    assert file_URL("KBGM20110612_003045_V03.gz")=="https://noaa-nexrad-level2.s3.amazonaws.com/2011/06/12/KBGM/KBGM20110612_003045_V03.gz"

def test_02():
    assert file_URL("KARX20100512_014240_V03.gz")=="https://noaa-nexrad-level2.s3.amazonaws.com/2010/05/12/KARX/KARX20100512_014240_V03.gz"

def test_03():
    assert file_URL("KABX20130902_002911_V06.gz")=="https://noaa-nexrad-level2.s3.amazonaws.com/2013/09/02/KABX/KABX20130902_002911_V06.gz"

def test_04():
    assert file_URL("KBIS20001222_090728.gz")=="https://noaa-nexrad-level2.s3.amazonaws.com/2000/12/22/KBIS/KBIS20001222_090728.gz"

def test_05():
    assert file_URL("KCCX20120203_013605_V03.gz")=="https://noaa-nexrad-level2.s3.amazonaws.com/2012/02/03/KCCX/KCCX20120203_013605_V03.gz"

def test_06():
    assert file_URL("KCBW20011213_002358.gz")==""

def test_07():
    assert file_URL("KBYX20150804_000940_V06.gz")=="https://noaa-nexrad-level2.s3.amazonaws.com/2015/08/04/KBYX/KBYX20150804_000940_V06.gz"

def test_08():
    assert file_URL("KAPX20120717_013219_V06.gz")=="https://noaa-nexrad-level2.s3.amazonaws.com/2012/07/17/KAPX/KAPX20120717_013219_V06.gz"

def test_09():
    assert file_URL("KAPX20140907_010223_V06.gz")=="https://noaa-nexrad-level2.s3.amazonaws.com/2014/09/07/KAPX/KAPX20140907_010223_V06.gz"

def test_10():
    assert file_URL("KCBW20080819_012424_V03.gz")=="https://noaa-nexrad-level2.s3.amazonaws.com/2008/08/19/KCBW/KCBW20080819_012424_V03.gz"

def test_11():
    assert file_URL("KLWX19931112_005128.gz")=="https://noaa-nexrad-level2.s3.amazonaws.com/1993/11/12/KLWX/KLWX19931112_005128.gz"

def test_12():
    assert file_URL("KBOX20030717_014732.gz")==""