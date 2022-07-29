
use std::rc::Rc;
use std::cell::RefCell;
use anyhow::*;
use rusqlite::{Connection, Row, Rows, Statement};

/*use pin_project::pin_project;
use std::pin::Pin;

extern crate owning_ref;
use owning_ref::{BoxRef, OwningRef};*/

use crate::anyhow_ext::*;
use crate::model::{BoundingBox, EntityCache};
use crate::queries::create_bbox;

// See:
// - https://www.reddit.com/r/rust/comments/dqa4t3/how_to_put_two_variables_one_borrows_from_other/
// - https://stackoverflow.com/questions/27552670/how-to-store-sqlite-prepared-statements-for-later
// - https://stackoverflow.com/questions/32300132/why-cant-i-store-a-value-and-a-reference-to-that-value-in-the-same-struct
// - https://stackoverflow.com/questions/49860149/how-to-use-the-pin-struct-with-self-referential-structures

/*const SQLQUERY_ALLMSLEVELS: &'static str = "SELECT bounding_box.* FROM bounding_box, spectrum WHERE spectrum.id = bounding_box.first_spectrum_id";
const SQLQUERY_SINGLEMSLEVEL: &'static str = "SELECT bounding_box.* FROM bounding_box, spectrum WHERE spectrum.id = bounding_box.first_spectrum_id AND spectrum.ms_level=?";

pub struct BoundingBoxIterator<'conn, 'cache> {
    stmt: Rc<RefCell<Statement<'conn>>>,
    rows: Rc<Rows<'conn>>,
    entity_cache: &'cache EntityCache,
}

impl<'conn, 'cache> BoundingBoxIterator<'conn, 'cache> {

    /*fn _create(rows: &'stmt mut Rows, entity_cache: &'cache EntityCache) -> Result<BoundingBoxIterator<'stmt, 'cache>> {
        Ok(BoundingBoxIterator {
            rows,
            entity_cache
        })
    }*/

    pub(crate) fn iterate_all_ms_levels(db: &'conn Connection, entity_cache: &'cache EntityCache) -> Result<BoundingBoxIterator<'conn, 'cache>> {
        let mut stmt: Rc<RefCell<Statement<'conn>>> = Rc::new(RefCell::new(db.prepare(SQLQUERY_ALLMSLEVELS).location(here!())?));
        let rows = stmt.borrow_mut().query([])?;

        let mut bb_iter = BoundingBoxIterator {
            stmt: stmt,
            rows: Rc::new(rows),
            entity_cache
        };

        Ok(bb_iter)
        //BoundingBoxIterator::_create(&mut stmt, entity_cache)
    }

    /*pub(crate) fn iterate_ms_level(db: &'conn Connection, ms_level: u8, entity_cache: &'cache EntityCache) -> Result<BoundingBoxIterator<'conn, 'stmt, 'cache>> {
        let mut stmt = db.prepare(SQLQUERY_SINGLEMSLEVEL).location(here!())?;
        let rows = stmt.query([ms_level])?;

        Ok(BoundingBoxIterator {
            stmt,
            rows,
            entity_cache
        })
        //BoundingBoxIterator::_create(&mut stmt, entity_cache)
    }*/
}*/

impl<'stmt, 'cache> BoundingBoxIterator<'stmt, 'cache> {

    pub(crate)  fn create(stmt: &'stmt mut Statement, entity_cache: &'cache EntityCache) -> Result<BoundingBoxIterator<'stmt, 'cache>> {
        let rows = stmt.query([])?;

        Ok(BoundingBoxIterator {
            rows,
            entity_cache
        })
    }

}

pub struct BoundingBoxIterator<'stmt, 'cache> {
    rows: Rows<'stmt>,
    entity_cache: &'cache EntityCache,
}

impl<'conn, 'cache> Iterator for BoundingBoxIterator<'conn, 'cache> {
    type Item = anyhow::Result<BoundingBox>;

    fn next(&mut self) -> Option<Self::Item> {
        let row_opt_res = self.rows.next();

        if row_opt_res.is_err() {
            return Some(Err(anyhow!("error during bounding box iteration: {}", row_opt_res.err().unwrap().to_string())))
        }

        let row_opt = row_opt_res.unwrap();

        let bb_opt = row_opt.map(|row| {
            let cur_bb = create_bbox(row).location(here!())?;

            Ok(cur_bb)
        });

        bb_opt
    }
}

pub fn create_bb_iter<'a>(db: &'a Connection, entity_cache: &'a EntityCache) -> Result<()> { // BoundingBoxIterator<'a, 'a>
    const SQLQUERY_ALLMSLEVELS: &'static str = "SELECT bounding_box.* FROM bounding_box, spectrum WHERE spectrum.id = bounding_box.first_spectrum_id";
    const SQLQUERY_SINGLEMSLEVEL: &'static str = "SELECT bounding_box.* FROM bounding_box, spectrum WHERE spectrum.id = bounding_box.first_spectrum_id AND spectrum.ms_level=?";

    let mut stmt = db.prepare(SQLQUERY_ALLMSLEVELS).location(here!())?;

    let bb_iter = BoundingBoxIterator::create(&mut stmt, entity_cache).location(here!())?;

    //let mut bb_iter = BoundingBoxIterator::iterate_all_ms_levels(&db, &entity_cache).location(here!())?;

    for bb_res in bb_iter {
        let bb = bb_res.location(here!())?;

        println!("{:?}", bb.id)
    }

    //Ok(bb_iter)

    Ok( ())
}


/*
pub struct BBIter<'stmt> {
    stmt: OwningRef<Box<Statement<'stmt>>, Statement<'stmt>>, //Box<Statement<'stmt>>,
    rows: Rows<'stmt>,
}

impl<'conn> BBIter<'conn> {

    pub(crate) fn iterate_all_ms_levels(db: &'conn Connection) -> Result<BBIter<'conn>> {
        let mut stmt: Box<Statement<'conn>> = Box::new(db.prepare("SELECT * FROM bounding_box").location(here!())? );
        let mut stmt_ref = BoxRef::new(stmt);
        let rows = stmt_ref.query([])?;

        let mut bb_iter = BBIter {
            stmt: stmt_ref,
            rows: rows
        };

        Ok(bb_iter)
        //BoundingBoxIterator::_create(&mut stmt, entity_cache)
    }
}
 */

/*
#[pin_project]
pub struct BBIter<'stmt> {
    #[pin]
    stmt: Statement<'stmt>, //Box<Statement<'stmt>>,
    rows: Rows<'stmt>,
}

impl<'conn> BBIter<'conn> {

    pub(crate) fn iterate_all_ms_levels(db: &'conn Connection) -> Result<BBIter<'conn>> {
        let mut stmt: Statement<'conn> = db.prepare("SELECT * FROM bounding_box").location(here!())?;
        let rows = stmt.query([])?;

        let mut bb_iter = BBIter {
            stmt: stmt,
            rows: rows
        };

        Ok(bb_iter)
        //BoundingBoxIterator::_create(&mut stmt, entity_cache)
    }
}*/