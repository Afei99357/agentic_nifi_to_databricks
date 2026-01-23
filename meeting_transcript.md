Bill Tucker
0 minutes 16 seconds0:16
Bill Tucker 0 minutes 16 seconds
Hey, Corey, can you hear me OK?

Cori Hendon
0 minutes 21 seconds0:21
Cori Hendon 0 minutes 21 seconds
Yeah, again. Good morning.

Bill Tucker
0 minutes 23 seconds0:23
Bill Tucker 0 minutes 23 seconds
I'm testing out some new headphones.
Bill Tucker 0 minutes 27 seconds
Trying to not wake the wake the kids and friends.

Cori Hendon
0 minutes 33 seconds0:33
Cori Hendon 0 minutes 33 seconds
Sounds like a great idea they'll appreciate.
EL

Eric Liao
0 minutes 39 seconds0:39
Eric Liao 0 minutes 39 seconds
Good money.

Cori Hendon
0 minutes 40 seconds0:40
Cori Hendon 0 minutes 40 seconds
Morning, guys.

Bill Tucker
0 minutes 42 seconds0:42
Bill Tucker 0 minutes 42 seconds
Howdy.

John Urban
0 minutes 44 seconds0:44
John Urban 0 minutes 44 seconds
Hey, Corey, good morning. Do do I need to be in that MXP? There's another meeting. Let me see what that is.
EL

Eric Liao
0 minutes 44 seconds0:44
Eric Liao 0 minutes 44 seconds
It.

John Urban
0 minutes 53 seconds0:53
John Urban 0 minutes 53 seconds
The Hadoop migration strategy. Maybe that was yesterday. I can't remember now.
Alexandru Muresan 1 hour 47 seconds
GPTOSS in Quantry and see if I if I managed to. But I think there's some pieces I would need to change because I the code is tailored for Opus. But yeah, I'll see what I can do. I'll try my best to to give them a spin and see if I get good results with them.

Cori Hendon
1 hour 1 minute 2 seconds1:01:02
Cori Hendon 1 hour 1 minute 2 seconds
OK, don't don't spend long on this. It's more of a get the endpoint up so we know we have a provision throughput option. If I was gonna run a test it would be can it generate a Databricks notebook like not not knife like version? Can it generate a Databricks notebook that can?

Alexandru Muresan
1 hour 1 minute 13 seconds1:01:13
Alexandru Muresan 1 hour 1 minute 13 seconds
Mhm.
Alexandru Muresan 1 hour 1 minute 18 seconds
Oh, OK.

Cori Hendon
1 hour 1 minute 22 seconds1:01:22
Cori Hendon 1 hour 1 minute 22 seconds
Be executed? Does does it know enough about Databricks to do that without just collapsing immediately? And and the the only the only other test that comes to mind would be is there going to be any built in API rate limiting beyond provision throughput which measures?

Alexandru Muresan
1 hour 1 minute 28 seconds1:01:28
Alexandru Muresan 1 hour 1 minute 28 seconds
Yeah, that makes sense.

Cori Hendon
1 hour 1 minute 42 seconds1:01:42
Cori Hendon 1 hour 1 minute 42 seconds
Tokens, right? So if I pick this model, I don't scale to zero. I can provision model units, which is an abstraction layer on tokens. Great, so I can make this be right, but.

Alexandru Muresan
1 hour 1 minute 53 seconds1:01:53
Alexandru Muresan 1 hour 1 minute 53 seconds
Mhm.

Cori Hendon
1 hour 1 minute 56 seconds1:01:56
Cori Hendon 1 hour 1 minute 56 seconds
Am I going to be bound by some? Here it can enforce rate limits on an endpoint, but is Databricks or Databricks service is going to enforce some other layer rate limits? Maybe, probably, but.
Cori Hendon 1 hour 2 minutes 12 seconds
Within the realm of what we're gonna hit is kind of the real question. Like if it can handle 100 requests a minute, OK, we're probably fine. Anyway, it's just the other place infrastructure falls over.

Alexandru Muresan
1 hour 2 minutes 19 seconds1:02:19
Alexandru Muresan 1 hour 2 minutes 19 seconds
Mhm.
Alexandru Muresan 1 hour 2 minutes 21 seconds
Yeah, I can.
Alexandru Muresan 1 hour 2 minutes 25 seconds
Yeah, I I'll start with the code generation, like you said, just something simple to see if we can use the models and then I'll see if if I hit any rate limits. I'll, you know, jot it down somewhere and let you guys know. If I don't hit any rate limits, I'll see if I can artificially make it, you know, as easy as possible, as quick as possible, hit some rate limits or some.
Alexandru Muresan 1 hour 2 minutes 45 seconds
Sort of context issues or something like that, yeah.

Cori Hendon
1 hour 2 minutes 46 seconds1:02:46
Cori Hendon 1 hour 2 minutes 46 seconds
Yeah.
Cori Hendon 1 hour 2 minutes 48 seconds
Oh, that's a good point. We measured this, but we didn't measure this one when we did it. Maybe I can just find the notebook and context window is usually different, right? How many tokens you can send to a prompt is different when it's done in Databricks on the foundation model endpoints.
Cori Hendon 1 hour 3 minutes 8 seconds
I don't know if we provision our own within Databricks if we're gonna have that same problem, but I think I've got a notebook where I can just point it at these on a provisioned throughput endpoint and see. So yeah, let me find that. I'll send it to you so you can stand it up on the.

Alexandru Muresan
1 hour 3 minutes 20 seconds1:03:20
Alexandru Muresan 1 hour 3 minutes 20 seconds
Is it?

Cori Hendon
1 hour 3 minutes 25 seconds1:03:25
Cori Hendon 1 hour 3 minutes 25 seconds
In XP side, yeah, and you can just run a notebook that we've already got.

Alexandru Muresan
1 hour 3 minutes 26 seconds1:03:26
Alexandru Muresan 1 hour 3 minutes 26 seconds
On the video, yeah.
Alexandru Muresan 1 hour 3 minutes 31 seconds
That's perfect.

Cori Hendon
1 hour 3 minutes 36 seconds1:03:36
Cori Hendon 1 hour 3 minutes 36 seconds
OK.
Cori Hendon 1 hour 3 minutes 38 seconds
Oh, thanks for that leaderboard link, Eric.
Cori Hendon 1 hour 3 minutes 49 seconds
The the audio's difficult. What are you saying?
EL
Eric Liao
1 hour 3 minutes 52 seconds1:03:52
Eric Liao 1 hour 3 minutes 52 seconds
I'm sorry, yes, it's still a little comparative with new ones. You get the benchmark, I mean it's not really fully trustworthy, but still at least it's some it's there's some some value there and we can use.
Eric Liao 1 hour 4 minutes 8 seconds
And Lama, I I remember at the very beginning I used to the field of the foundation module, they're break serving for the Lama version. The Lama's actually pretty bad compared to even the even Cloud August 4. So I think yeah, that's pretty clear now there for me.

Cori Hendon
1 hour 4 minutes 22 seconds1:04:22
Cori Hendon 1 hour 4 minutes 22 seconds
Yeah.
Cori Hendon 1 hour 4 minutes 27 seconds
Yeah, OK.
EL
Eric Liao
1 hour 4 minutes 28 seconds1:04:28
Eric Liao 1 hour 4 minutes 28 seconds
Mhm.

Cori Hendon
1 hour 4 minutes 30 seconds1:04:30
Cori Hendon 1 hour 4 minutes 30 seconds
Cool.
Cori Hendon 1 hour 4 minutes 33 seconds
Alright, I think.
Cori Hendon 1 hour 4 minutes 37 seconds
OK, we we do have a little time. I thought there was some other meeting we had to jump to. Not yet.
Cori Hendon 1 hour 4 minutes 43 seconds
OK. Any anything else guys? Do we have at least some decent direction in parallel work streams now to start putting this together?
Cori Hendon 1 hour 4 minutes 59 seconds
OK, let me rephrase it. Do do we have any questions or needs to be productive either for the rest of today or see you when you come in Monday and we're not here yet, do we know what what we're working on?

Alex Sisu
1 hour 5 minutes 13 seconds1:05:13
Alex Sisu 1 hour 5 minutes 13 seconds
We know what we work on on the US side.

Cori Hendon
1 hour 5 minutes 15 seconds1:05:15
Cori Hendon 1 hour 5 minutes 15 seconds
All right.

John Urban
1 hour 5 minutes 18 seconds1:05:18
John Urban 1 hour 5 minutes 18 seconds
We got good direction, Corey, Bill and I. Thanks.
EL
Eric Liao
1 hour 5 minutes 23 seconds1:05:23
Eric Liao 1 hour 5 minutes 23 seconds
Yeah, I think, yeah, I think I can just start working on their first draft and then we can see what's look like in the future 1st and then stuff on there and maybe we have more clear clue when if you can see something visually maybe.

Cori Hendon
1 hour 5 minutes 23 seconds1:05:23
Cori Hendon 1 hour 5 minutes 23 seconds
All right.
Cori Hendon 1 hour 5 minutes 36 seconds
All right, cool.
Cori Hendon 1 hour 5 minutes 38 seconds
Then reach out if you need stuff. I'll go look for that token limit script. Nothing fancy, it's just a search and I'll I'll share it with you here soon.
EL
Eric Liao
1 hour 5 minutes 44 seconds1:05:44
Eric Liao 1 hour 5 minutes 44 seconds
Mm.

Alex Sisu
1 hour 5 minutes 52 seconds1:05:52
Alex Sisu 1 hour 5 minutes 52 seconds
Thank you.
Alex Sisu 1 hour 5 minutes 54 seconds
Bye, bye. Thanks.

Speaker 1 stopped transcription