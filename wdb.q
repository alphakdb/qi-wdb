/ adapted from https://github.com/simongarland/tick/blob/master/w.q
.qi.import`event
.qi.import`cron

KOE:any`keeponexit`koe in key .qi.opts
gettmppath:{.qi.path(.qi.getconf[`tmpPath;.conf.DATA,"/",string[.proc.self.stackname],"/tmp"];"wdb_",string[.z.i],"_",.qi.tostr[x]except".")}
getsymenumpath:{.qi.path(.qi.getconf[`tmpPath;.conf.DATA,"/",string[.proc.self.stackname],"/tmp"];"symenum_",string[.z.i],"_",.qi.tostr[x]except".")}
writetmp:{.[.qi.path(TMPPATH;x;`);();,;.Q.en[SYMENUMPATH]`. x]}
clearall:{@[`.;tables`;0#]}
writeandclear:{writetmp each t:a where 0<(count get@)each a:tables`;clearall`;.qi.info"flushed ",string[count t]," table(s) to disk"}
writeall:{.qi.info"moving tables out of memory and onto disk at: ",(8#2_string .z.n)," UTC";writeandclear`}
memcheck:{if[(1024*1024*.conf.WDB_MAXMB)<.Q.w[]`used;writeandclear`]}

append:{[t;data]
    if[t in tables`;t insert data;
    if[not`g=attr get[t]`sym;update`g#sym from t];
    if[.conf.MAXROWS<count get t;writeandclear`]]
 }

upd:append

disksort:{[t;c;a]
    if[not`s~attr(t:hsym t)c; / if its already sorted we skip everything (no need to sort a sorted list)
        if[count t; / if the table is empty, there is nothing to sort
            ii:iasc iasc flip c!t c,:(); / this tells you the index each number needs to go in order for the list to be sorted
            if[not$[(0,-1+count ii)~(first;last)@\:ii;@[{`s#x;1b};ii;0b];0b]; / if the first and last indices are 0&N-1. then it might be sorted. try to apply the sorted attribute 
               {v:get y;if[not$[all(fv:first v)~/:256#v;all fv~/:v;0b];v[x]:v;y set v];}[ii]each` sv't,'get` sv t,`.d / on each column file within each tmp
              ]
          ];
        @[t;first c;a] / apply the parted attribute on each sym col
      ];t}

.u.end:{ / end of day: save, clear, sort on disk, backup sym, promote sym, move, hdb reload
    writeandclear`;
    {disksort[.qi.path(TMPPATH;x;`);`sym;`p#]}each key TMPPATH;
    / backup HDB sym
    .qi.os.ensuredir bkpdir:.qi.path(SYMBACKUPDIR;string x);
    .qi.info"Backing up HDB sym to: ",.qi.ospath .qi.path(bkpdir;`sym.bkp);
    if[.qi.exists hdbsym:.qi.path(.wdb.hdb_dir;"sym");
        .qi.os.cpfile[hdbsym;.qi.path(bkpdir;`sym.bkp)]];
    / promote working sym into HDB
    .qi.info"Promoting updated sym to HDB";
    .qi.os.ensuredir .wdb.hdb_dir;
    .qi.os.mv[.qi.ospath .qi.path(SYMENUMPATH;`sym);.qi.ospath hdbsym];
    / move new partition into HDB
    .qi.os.ensuredir p:.qi.path(.wdb.hdb_dir;x);
    .qi.os.mv[.qi.ospath(TMPPATH;"*");p];
    / clean up empty tmp dirs and roll globals for new day
    hdel each (TMPPATH;SYMENUMPATH);
    TMPPATH::gettmppath .z.d;
    SYMENUMPATH::getsymenumpath .z.d;
    initsymenum[];
    .Q.gc[];
    $[null h:.ipc.conn .wdb.hdb;
        .qi.info "Could not connect to ",string[.wdb.hdb]," to initiate reload";
        [.qi.info "Initiating reload on ",string .wdb.hdb;
         h"reload[]"]];
    }

.z.exit:{if[not KOE;writeandclear`]} 

initsymenum:{
    .qi.os.ensuredir SYMENUMPATH;
    if[.qi.exists hdbsym:.qi.path(.wdb.hdb_dir;`sym);
        .qi.os.cpfile[hdbsym;.qi.path(SYMENUMPATH;`sym)]];
    }

/ connect to ticker plant for (schema;(logcount;log))
.wdb.init:{
    if[(::)~.wdb.hdb:.qi.tosym .proc.self.options`hdb;
    '"A wdb process needs a hdb entry in its process config"];
    .wdb.hdb_dir:.qi.path(.conf.DATA;.proc.self.stackname;`hdb;.wdb.hdb);
    TMPPATH::gettmppath .z.d;
    SYMENUMPATH::getsymenumpath .z.d;
    SYMBACKUPDIR::.qi.getconf[`symBackupDir;.conf.DATA,"/",string[.proc.self.stackname],"/symbackups"];
    if[null .proc.self.mystack[.wdb.hdb;`pkg];show .proc.self.mystack;'string[.wdb.hdb]," not found"];
    initsymenum[];
    .proc.subinitreplay[];
    .cron.add[`writeall;.z.p;.conf.WRITE_EVERY];
    .cron.add[`memcheck;.z.p;.conf.MEM_CHECK_EVERY];
    .event.addhandler[`.z.ts;`.cron.run];
    .cron.start[];
    }

tcounts:.qi.tcounts