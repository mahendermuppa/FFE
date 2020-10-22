import React from 'react';
import { Link } from 'react-router-dom'
import './style.css';

 const Navbar = ()=>{
    return(
            <nav className="nav-wrapper">
                <div className="container">
                    
                    <ul className="left">
                        <li><Link to="/">Products</Link></li>
                    </ul>
                    <ul className="right">
                        <li><Link to="/">Order Status</Link></li> 
                        <li><Link to="/cart">My cart&nbsp;<i className="material-icons displayBlk">shopping_cart</i></Link></li>
                    </ul>
                </div>
            </nav>
   
        
    )
}

export default Navbar;