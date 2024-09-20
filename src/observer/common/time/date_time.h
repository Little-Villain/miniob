bool check_date(int y,int m,int d)
{
    static int mon[]={0,31,28,31,30,31,30,31,31,30,31,30,31};
    bool leap= (y%400==0 ||( y%100 && y%4==0));
    return y>0 && y<=9999
        && (m>0) && (m<=12)
        && (d>0) && ( d<= ( (m==2&&leap)?1:0 )+mon[m]);
}