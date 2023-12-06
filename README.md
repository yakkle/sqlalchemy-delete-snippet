# Delete parent when all children deleted with before_flush event

https://groups.google.com/g/sqlalchemy/c/yGqvmu1vr6E

> One event approach would be the before_flush() event, you search through session.deleted for all Container objects; within Container, locate all Tag entries with that Container as the ultimate parent and mark them all as deleted as well.   With the before_flush() approach, you can use the Session and your objects in the usual way.

# Alternatives
 - https://stackoverflow.com/a/64401347/7155818
 - https://stackoverflow.com/q/51419186/7155818

 # References
 - https://stackoverflow.com/q/5033547/7155818
 - https://www.peterspython.com/en/blog/sqlalchemy-using-cascade-deletes-to-delete-related-objects
