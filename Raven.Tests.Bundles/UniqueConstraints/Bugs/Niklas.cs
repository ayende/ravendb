using Raven.Client.UniqueConstraints;
using Raven.Tests.Bundles.Authorization.Bugs;
using Xunit;

namespace Raven.Tests.Bundles.UniqueConstraints.Bugs
{

    public class Niklas : UniqueConstraintsTest
    {

        public class User
        {

            public string Id { get; set; }

            [UniqueConstraint]
            public string Name { get; set; }
        }

        [Fact]
        public void LoadByUiqueConstraint_Fails_To_Load_Second_Time_After_Creating()
        {
            var user = new User {Name = "Niklas"};

            using (var session = DocumentStore.OpenSession())
            {
                // Works if I remove this line
                session.LoadByUniqueConstraint<User>(x => x.Name, user.Name);

                session.Store(user);
                session.SaveChanges();

                var found = session.LoadByUniqueConstraint<User>(x => x.Name, user.Name);
                Assert.NotNull(found);
            }

        }

    }

}
