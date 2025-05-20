const express = require('express')
const app = express();
const path = require("path");
const fs=require('fs');
 const port =3000

app.set("view engine","ejs");
app.use(express.json());
app.use(express.urlencoded({extended:true}));

app.use(express.static(path.join(__dirname,"public")));//static file ko access karne k liye 



app.get('/',(req,res)=>{
    fs.readdir(`./files`,function(err,files){
   res.render('index',{files:files})
    })
})

app.get('/edit/:filename',(req,res)=>{
   res.render('edit',{filename:req.params.filename});
})

app.post('/edit',(req,res)=>{
fs.rename(`./files/${req.body.previous_title}`,`./files/${req.body.new_title}`,function(err){
    res.redirect('/');
    console.log(req.body);
})
}) 

app.post('/create',(req,res)=>{
  fs.writeFile(`./files/${req.body.title.split(' ').join('')}.txt`,req.body.description,function(err){
    res.redirect("/");
  })
})

app.get('/files/:filename',(req,res)=>{
  fs.readFile(`./files/${req.params.filename}`, "utf-8", function(err,filedata){
    res.render('show',{filename:req.params.filename,filedata:filedata})
  })
})

app.listen(port , function(){
    console.log(`app is running on the port ${port}`)
})