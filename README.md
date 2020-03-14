# discogsextractor

*Download all the modules*
 ```
 cd ~/downloads/
 
 wget https://cpan.metacpan.org/authors/id/J/JV/JVANNUCCI/Parallel-Fork-BossWorkerAsync-0.09.tar.gz
 wget https://cpan.metacpan.org/authors/id/T/TI/TIMB/DBI-1.643.tar.gz
 wget https://cpan.metacpan.org/authors/id/T/TU/TURNSTEP/DBD-Pg-3.10.4.tar.gz
 wget https://cpan.metacpan.org/authors/id/O/OA/OALDERS/libwww-perl-6.43.tar.gz
 wget https://cpan.metacpan.org/authors/id/M/MA/MANWAR/XML-XPath-1.44.tar.gz
 wget https://cpan.metacpan.org/authors/id/O/OA/OALDERS/URI-1.76.tar.gz
 wget https://cpan.metacpan.org/authors/id/D/DA/DANKOGAI/Encode-3.04.tar.gz
 wget https://cpan.metacpan.org/authors/id/M/MS/MSCHILLI/Log-Log4perl-1.49.tar.gz
 wget https://cpan.metacpan.org/authors/id/E/ET/ETHER/Try-Tiny-0.30.tar.gz
 wget https://cpan.metacpan.org/authors/id/I/IS/ISHIGAKI/JSON-4.02.tar.gz 
 ```
 
*unzip all the modules in src directory*
 ```
cd ~/src
tar -zxvf ~/downloads/*
tar -zxvf ~/downloads/*.gz
tar -zxvf ~/downloads/DBI-1.643.tar.gz
tar -zxvf ~/downloads/DBD-Pg-3.10.4.tar.gz
tar -zxvf ~/downloads/Encode-3.04.tar.gz
tar -zxvf ~/downloads/JSON-4.02.tar.gz
tar -zxvf ~/downloads/libwww-perl-6.43.tar.gz
tar -zxvf ~/downloads/Log-Log4perl-1.49.tar.gz
tar -zxvf ~/downloads/Parallel-Fork-BossWorkerAsync-0.09.tar.gz
tar -zxvf ~/downloads/Try-Tiny-0.30.tar.gz
tar -zxvf ~/downloads/URI-1.76.tar.gz
tar -zxvf ~/downloads/XML-XPath-1.44.tar.gz
```

```
$ ls
DBD-Pg-3.10.4  DBI-1.643  Encode-3.04  JSON-4.02  libwww-perl-6.43  Log-Log4perl-1.49  Parallel-Fork-BossWorkerAsync-0.09  Try-Tiny-0.30  URI-1.76  XML-XPath-1.44
```


*Install each modules like this with local prefix*
```
cd DBI-1.643/
perl Makefile.PL PREFIX=~/lib/perl5
make
make test
make install
```
*set PERL5LIB to point to installed module directory*
```
setenv PERL5LIB /ilab/users/gjamuar/lib/perl5/share/
```
