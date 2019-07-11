﻿using System;

namespace MarineResearch
{
    class Program
    {
        static void Main(string[] args)
        {
            using (var client = new MarineResearchTest(args[0], "PutCommentsTest", -1, Guid.NewGuid().ToString()))
            {
                client.Initialize();
                client.RunTest();
            }
        }
    }
}
