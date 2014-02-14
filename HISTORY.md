
# olap4j-xmlaserver

#############################################################################
## Version 1.2.0

This is the first release of olap4j-xmlaserver. We have numbered it 1.2.0 so
it aligns with the core olap4j releases. Future releases might not maintain
this alignment.

This release is also the first to be compatible with OSGI. For a list
of the exported packages, please refer to the maven build file.

#############################################################################

## Commit history

[4c9a99f](../../commit/4c9a99f50b5cbf1eb5959d0fe230cc25e93a7bfa)
Fri, 10 Jan 2014 11:40:13 -0500 - __(Kurtis Walker)__
Mondrian needs some additional packages exported in order to fully resolve in OSGI.  This changes makes the exported xmlaserver packages explicit.

[33fa613](../../commit/33fa6135672db30de665d568521c4faf962f3760)
Thu, 9 Jan 2014 10:49:05 -0500 - __(Luc Boudreau)__
Update pom.xml

[2939112](../../commit/29391127e752ce457f766f9009f23bca1e411bf0)
Thu, 5 Dec 2013 09:51:32 -0500 - __(Nicholas Baker)__
Changes to POM to make the artifact an OSGI bundle. Upped version to 0.0.2-SNAPSHOT due to the change.

[8f0f55f](../../commit/8f0f55fa2dbb68df2ea8795a40d34ba7d830ec75)
Tue, 26 Nov 2013 10:39:26 -0500 - __(Kurtis Walker)__
ANALYZER-2188 - adding new methods to XmlaExtra

[dad5a17](../../commit/dad5a17cb38d2fa713e2be89c1748f4b6bf37813)
Fri, 26 Jul 2013 10:12:07 -0700 - __(Julian Hyde)__
Remove Intellij files from git.

[d7dfa1f](../../commit/d7dfa1fce7c1791e628d7dafe22329205aaf2ef5)
Wed, 19 Jun 2013 09:18:32 -0400 - __(mkambol)__
[MONDRIAN-1581] Fixing datatypes which were incorrectly marshalled in XMLA responses:  Boolean, Short, Byte, Float (were formerly treated as xsd:string, xsd:int, xsd:int, and xsd:double, respectively) merged from 534768

[a0e99cd](../../commit/a0e99cd8df51d7650213af3de0c2c46ca1f7edde)
Thu, 21 Feb 2013 10:34:15 -0500 - __(Luc Boudreau)__
Merge of commit 24cf3d0e2a57b88bb9550e4bccd8c8bb34c6d9db form Mondrian's master.

[34e6697](../../commit/34e66971c15a947ec99e8a56c6273d0c2de1648c)
Thu, 21 Feb 2013 10:20:56 -0500 - __(Luc Boudreau)__
Merge of change a5c552f74ed8068a7b40fa6f2b3e9a995d94e100 from Mondrian's master.

[2f444e2](../../commit/2f444e26e905804fd13b5d53636d168b6df925ed)
Thu, 21 Feb 2013 09:39:15 -0500 - __(Luc Boudreau)__
Merge of changes 8fa75e276f638b1156e0fa1c447fc49006112f88 ba93ad762db859d04d6aa2411e4587280ca06249 160dadad7af7dee209fa85ba8cbd9106fa60c799 from Mondrian's master.

[d87b6ad](../../commit/d87b6ad54c3ba39448ec3bc2a31a249d75d9acf5)
Wed, 2 Jan 2013 11:59:47 -0500 - __(Luc Boudreau)__
Adds getSchemaId() to XmlaExtra.

[1cd132a](../../commit/1cd132ad3a5f225412ba8ed8cb9c4409c59653a4)
Mon, 17 Dec 2012 14:31:27 -0500 - __(Luc Boudreau)__
Idem.

[04296ff](../../commit/04296ff62f7fee2531b33d342fe2a1c8cd9b118d)
Mon, 17 Dec 2012 13:30:45 -0500 - __(buildguy)__
Update pom.xml

[c620eab](../../commit/c620eab7bb69916df36944963c0a94169b497eb4)
Mon, 17 Dec 2012 12:56:50 -0500 - __(Luc Boudreau)__
Idem.

[a6f4059](../../commit/a6f405959235692e064210b1333a7e4abe1ec038)
Mon, 17 Dec 2012 12:54:58 -0500 - __(Luc Boudreau)__
Idem.

[773507d](../../commit/773507d0affdba351fc50e37c91479e7f3fe9633)
Mon, 17 Dec 2012 12:43:44 -0500 - __(Luc Boudreau)__
An attempt to fix CI.

[fae048f](../../commit/fae048f973b8aa51d4c9abee9c707042fedf33df)
Mon, 17 Dec 2012 11:29:18 -0500 - __(Luc Boudreau)__
Small change to the pom to get deployment working on CI.

[ffd4216](../../commit/ffd4216b41e1e0009df0ade94d2125bf984919f4)
Mon, 17 Dec 2012 10:54:36 -0500 - __(Luc Boudreau)__
Upgrades to latest olap4j.

[38d065a](../../commit/38d065af43a45335758d772072066cafea9159d7)
Fri, 7 Sep 2012 15:48:26 -0700 - __(Julian Hyde)__
DefaultSaxWriter now writes to any Appendable, rather than a PrintWriter. If the Appendable is a StringBuilder, this should be more efficient.

[ed28a49](../../commit/ed28a4973c69fe0991c63b9e4a55b0faf87d3cb0)
Fri, 7 Sep 2012 13:25:31 -0700 - __(Julian Hyde)__
Fix name of project in pom; get XmlaExtra from ConnectionFactory, rather than by using "unwrap" on the OlapConnection.

[de2fed9](../../commit/de2fed93ee14d5ae4a83a34b12bda513ff6d2e63)
Fri, 31 Aug 2012 21:07:17 -0700 - __(Julian Hyde)__
Slim down dependencies.

[3602acf](../../commit/3602acf4133dabf48a053fdb7b5186eaee7a97de)
Fri, 31 Aug 2012 20:46:51 -0700 - __(Julian Hyde)__
Remove mondrian dependencies. (Extend XmlaExtra interface, remove stub classes.)

[7e53b09](../../commit/7e53b09f6820d86ad38d552b71316fd05df0da61)
Fri, 31 Aug 2012 17:03:32 -0700 - __(Julian Hyde)__
Initial version, copying mondrian.xmla package and stub versions of mondrian classes it depends upon.

[3600730](../../commit/3600730867ac523c8cc0cbcc346b711ff5d1c42a)
Fri, 31 Aug 2012 15:20:27 -0700 - __(Julian Hyde)__
Initial commit
