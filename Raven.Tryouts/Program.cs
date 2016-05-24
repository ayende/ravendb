using System;
using System.Collections.Generic;
using System.Linq;
using Lucene.Net.Support;
using Raven.Client.Document;

#if !DNXCORE50

#endif

namespace Raven.Tryouts
{
    enum Ts
    {
        StartTrackingField
    }

    public class Program
    {
        public static void Main(string[] args)
        {
#if !DNXCORE50

            var store = new DocumentStore
            {
                DefaultDatabase = "Linux",
                Url = "http://localhost:8080"
            };
            store.Initialize();

            var car = new Car(new List<Owner>())
            {
                Make = "Ford",
                CurrentOwner = new Owner { FirstName = "Ned" },
                PreviousOwner = new Owner { FirstName = "Adama" }
            };
            // With the constructor initializing this list, if you add a value to it then the FieldName will be correct for PreviousOwner.FirstName
            car.Owners.Add(new Owner { FirstName = "Sam" });
            car.Owners.Add(new Owner { FirstName = "Dan" });
            car.SecondeOwners.Add(new Owner { FirstName = "first",StringTest = new List<string>
            {
                { "ssss"},
                {"ssdsdsds" }

    
            }});

            var carId = "Car/1";

            using (var session = store.OpenSession())
            {
                session.Store(car, carId);
                session.SaveChanges();
            }

            
            using (var session = store.OpenSession())
            {
                var returnedCar = session.Load<Car>(carId);
                returnedCar.CurrentOwner.FirstName = "Arya";
                returnedCar.PreviousOwner.FirstName = "Starbuck";
                // If you change the value of a List item, then the FieldName will NOT be correct for the List item
                returnedCar.Owners[0].FirstName = "Frodo";
                returnedCar.Owners[1].FirstName = "Auriel";
                returnedCar.SecondeOwners[0].FirstName = "firstfirst";
                returnedCar.SecondeOwners[0].StringTest[0] = "stringtest";
                returnedCar.SecondeOwners[0].StringTest[1] = "stringtestssss";

                //                returnedCar.SecondeOwners.Add(new Owner {FirstName = "second"});
                //                returnedCar.Owners[1].FirstName = "Dana";
                //                returnedCar.Owners.Add(new Owner {FirstName = "killian"});

                var whatChanged = session.Advanced.WhatChanged().Where(changes => changes.Key == returnedCar.Id);
                Console.WriteLine(whatChanged);
            }
#endif
        }
    }


    public class Car
    {
        public string Id { get; set; }
        public string Make { get; set; }
        public Owner CurrentOwner { get; set; }
        public Owner PreviousOwner { get; set; }
        public List<Owner> Owners { get; set; }
        public List<Owner> SecondeOwners { get; set; }

        public Car() { }

        public Car(List<Owner> owners)
        {
            Owners = owners;
            SecondeOwners = new List<Owner>();
        }
    }

    public class Owner
    {
        public string FirstName { get; set; }
        public string LastName { get; set; }
        public List<string> StringTest { get; set; }
    }

    public class Lissing
    {
        public Owner Owner { get; set; }
    }
}
